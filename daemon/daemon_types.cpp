#include "inc/daemon_types.hpp"
#include "logging.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace cachebank {

/** Client */
Client::Client(uint64_t id_, uint64_t region_limit)
    : status(ClientStatusCode::INIT), id(id_), region_cnt_(0),
      region_limit_(region_limit),
      cq(utils::get_ackq_name(kNameCtrlQ, id), false),
      txqp(std::to_string(id), false) {}

Client::~Client() {
  cq.destroy();
  txqp.destroy();
  destroy();
}

void Client::connect() {
  CtrlMsg ack{.op = CtrlOpCode::CONNECT, .ret = CtrlRetCode::CONN_SUCC};
  cq.send(&ack, sizeof(ack));
}

void Client::disconnect() {
  CtrlMsg ret_msg{.op = CtrlOpCode::DISCONNECT, .ret = CtrlRetCode::CONN_SUCC};
  cq.send(&ret_msg, sizeof(ret_msg));
}

/** @overcommit: evacuator might request several overcommitted regions for
 *    temporary usages. It will soon return more regions back.
 */
void Client::alloc_region_(size_t size, bool overcommit) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;

  if (overcommit || region_cnt_ < region_limit_) {
    int64_t region_id = new_region_id_();
    const auto rwmode = boost::interprocess::read_write;
    const std::string chunkname = utils::get_region_name(id, region_id);

    if (regions.find(region_id) == regions.cend()) {
      int64_t actual_size;
      auto region = std::make_shared<SharedMemObj>(
          boost::interprocess::create_only, chunkname.c_str(), rwmode);
      regions[region_id] = region;
      region_cnt_++;

      region->truncate(size);
      region->get_size(actual_size);

      ret = CtrlRetCode::MEM_SUCC;
      mm.region_id = region_id;
      mm.size = actual_size;
    } else {
      /* region has already existed */
      LOG(kError) << "Client " << id << " has already allocated region "
                  << region_id;

      ret = CtrlRetCode::MEM_FAIL;
    }
  }

  CtrlMsg ret_msg{.op = CtrlOpCode::ALLOC, .ret = ret, .mmsg = mm};
  cq.send(&ret_msg, sizeof(ret_msg));
}

void Client::free_region(int64_t region_id) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;
  int64_t actual_size;

  auto region_iter = regions.find(region_id);
  if (region_iter != regions.end()) {
    /* Successfully find the region to be freed */
    region_iter->second->get_size(actual_size);
    regions.erase(region_id);
    region_cnt_--;

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    LOG(kError) << "Client " << id << " doesn't have region " << region_id;
    ret = CtrlRetCode::MEM_FAIL;
  }

  CtrlMsg ack{.op = CtrlOpCode::FREE, .ret = ret, .mmsg = mm};
  cq.send(&ack, sizeof(ack));
}

void Client::destroy() {
  for (const auto &kv : regions) {
    const std::string name = utils::get_region_name(id, kv.first);
    SharedMemObj::remove(name.c_str());
  }
}

/** Daemon */
Daemon::Daemon(const std::string ctrlq_name)
    : ctrlq_name_(utils::get_rq_name(ctrlq_name, true)) {
  MsgQueue::remove(ctrlq_name_.c_str());
  ctrlq_ = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                      ctrlq_name_.c_str(), kDaemonQDepth,
                                      sizeof(CtrlMsg));
}

Daemon::~Daemon() {
  clients_.clear();
  MsgQueue::remove(ctrlq_name_.c_str());
}

int Daemon::do_connect(const CtrlMsg &msg) {
  try {
    if (clients_.find(msg.id) != clients_.cend()) {
      LOG(kError) << "Client " << msg.id << " connected twice!";
      /* TODO: this might be some stale client. Probably we could try to
       * send the ack back */
      return -1;
    }
    // clients_[msg.id] = std::move(Client(msg.id));
    clients_.insert(std::make_pair(msg.id, Client(msg.id, kRegionLimit)));
    auto client_iter = clients_.find(msg.id);
    assert(client_iter != clients_.cend());
    auto &client = client_iter->second;
    LOG(kInfo) << "Client " << msg.id << " connected.";
    client.connect();
  } catch (boost::interprocess::interprocess_exception &e) {
    LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_disconnect(const CtrlMsg &msg) {
  try {
    auto client_iter = clients_.find(msg.id);
    if (client_iter == clients_.cend()) {
      /* TODO: this might be some unregistered client. Probably we could try to
       * connect to it and send the ack back */
      LOG(kError) << "Client " << msg.id << " doesn't exist!";
      return -1;
    }
    client_iter->second.disconnect();

    clients_.erase(msg.id);
    LOG(kInfo) << "Client " << msg.id << " disconnected!";
  } catch (boost::interprocess::interprocess_exception &e) {
    LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_alloc(const CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  auto &client = client_iter->second;
  assert(msg.id == client.id);
  client.alloc_region(msg.mmsg.size);

  return 0;
}

int Daemon::do_overcommit(const CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  auto &client = client_iter->second;
  assert(msg.id == client.id);
  client.overcommit_region(msg.mmsg.size);

  return 0;
}

int Daemon::do_free(const CtrlMsg &msg) {
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }

  uint64_t region_id = msg.mmsg.region_id;
  size_t region_size = msg.mmsg.size;
  const std::string chunkname = utils::get_region_name(msg.id, region_id);
  auto &client = client_iter->second;
  client.free_region(region_id);

  return 0;
}

void Daemon::serve() {
  LOG(kInfo) << "Daemon starts listening...";

  while (true) {
    CtrlMsg msg;
    size_t recvd_size;
    unsigned int prio;

    ctrlq_->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      break;
    }
    LOG(kInfo) << "Daemon recved msg " << msg.op;
    switch (msg.op) {
    case CONNECT:
      do_connect(msg);
      break;
    case DISCONNECT:
      do_disconnect(msg);
      break;
    case ALLOC:
      do_alloc(msg);
      break;
    case OVERCOMMIT:
      do_overcommit(msg);
      break;
    case FREE:
      do_free(msg);
      break;
    default:
      LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }
}

} // namespace cachebank
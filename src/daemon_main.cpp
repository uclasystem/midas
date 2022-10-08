#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>

#include "logging.hpp"
#include "qpair.hpp"
#include "utils.hpp"
#include "shm_types.hpp"

namespace cachebank {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;

class Daemon;

class Client {
public:
  Client(uint64_t id_)
      : status(ClientStatusCode::INIT), id(id_), _region_cnt(0),
        cq(utils::get_ackq_name(kNameCtrlQ, id), false),
        txqp(std::to_string(id), false) {}
  ~Client() {
    cq.destroy();
    txqp.destroy();
    unmap_regions_();
  }

  uint64_t id;
  ClientStatusCode status;
  QSingle cq; // per-client completion queue for the ctrl queue
  QPair txqp;
  std::unordered_map<int64_t, std::shared_ptr<SharedMemObj>> regions;

private:
  friend class Daemon;
  inline int64_t new_region_id() noexcept { return _region_cnt++; }
  std::pair<CtrlRetCode, MemMsg> alloc_region(size_t size);
  std::pair<CtrlRetCode, MemMsg> free_region(int64_t region_id);
  void unmap_regions();

  uint64_t _region_cnt;
};

std::pair<CtrlRetCode, MemMsg> Client::alloc_region(size_t size) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;

  int64_t region_id = new_region_id();
  const auto rwmode = boost::interprocess::read_write;
  const std::string chunkname = utils::get_region_name(id, region_id);

  if (regions.find(region_id) == regions.cend()) {
    int64_t actual_size;
    auto region = std::make_shared<SharedMemObj>(
        boost::interprocess::create_only, chunkname.c_str(), rwmode);
    regions[region_id] = region;

    region->truncate(size);
    region->get_size(actual_size);

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    LOG(kError) << "Client " << id << " has already allocated region "
                << region_id;

    ret = CtrlRetCode::MEM_FAIL;
  }
  return std::make_pair(ret, std::move(mm));
}

std::pair<CtrlRetCode, MemMsg> Client::free_region(int64_t region_id) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;
  int64_t actual_size;

  auto region_iter = regions.find(region_id);
  if (region_iter != regions.end()) {
    /* Successfully find the region to be freed */
    region_iter->second->get_size(actual_size);
    regions.erase(region_id);

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    LOG(kError) << "Client " << id << " doesn't have region " << region_id;
    ret = CtrlRetCode::MEM_FAIL;
  }

  return std::make_pair(ret, std::move(mm));
}

void Client::unmap_regions() {
  for (const auto &kv : regions) {
    const std::string name = utils::get_region_name(id, kv.first);
    SharedMemObj::remove(name.c_str());
  }
}

class Daemon {
public:
  Daemon(const std::string ctrlq_name = kNameCtrlQ)
      : _ctrlq_name(utils::get_rq_name(ctrlq_name, true)) {
    MsgQueue::remove(_ctrlq_name.c_str());
    _ctrlq = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                        _ctrlq_name.c_str(), kDaemonQDepth,
                                        sizeof(CtrlMsg));
  }
  ~Daemon() {
    _clients.clear();
    MsgQueue::remove(_ctrlq_name.c_str());
  }
  void serve();

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);

  const std::string _ctrlq_name;
  std::shared_ptr<MsgQueue> _ctrlq;
  std::unordered_map<uint64_t, Client> _clients;
};

int Daemon::do_connect(const CtrlMsg &msg) {
  try {
    if (_clients.find(msg.id) != _clients.cend()) {
      LOG(kError) << "Client " << msg.id << " connected twice!";
      /* TODO: this might be some stale client. Probably we could try to
       * send the ack back */
      return -1;
    }
    // _clients[msg.id] = std::move(Client(msg.id));
    _clients.insert(std::make_pair(msg.id, Client(msg.id)));
    auto client_iter = _clients.find(msg.id);
    assert(client_iter != _clients.cend());
    auto &client = client_iter->second;
    LOG(kInfo) << "Client " << msg.id << " connected.";

    CtrlMsg ack{.op = CtrlOpCode::CONNECT, .ret = CtrlRetCode::CONN_SUCC};
    client.cq.send(&ack, sizeof(ack));
  } catch (boost::interprocess::interprocess_exception &e) {
    LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_disconnect(const CtrlMsg &msg) {
  try {
    auto client_iter = _clients.find(msg.id);
    if (client_iter == _clients.cend()) {
      /* TODO: this might be some unregistered client. Probably we could try to
       * connect to it and send the ack back */
      LOG(kError) << "Client " << msg.id << " doesn't exist!";
      return -1;
    }

    CtrlMsg ret_msg{.op = CtrlOpCode::DISCONNECT,
                    .ret = CtrlRetCode::CONN_SUCC};
    client_iter->second.cq.send(&ret_msg, sizeof(ret_msg));

    _clients.erase(msg.id);
    LOG(kInfo) << "Client " << msg.id << " disconnected!";
  } catch (boost::interprocess::interprocess_exception &e) {
    LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_alloc(const CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto client_iter = _clients.find(msg.id);
  if (client_iter == _clients.cend()) {
    /* TODO: same as in do_disconnect */
    LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  auto &client = client_iter->second;
  assert(msg.id == client.id);

  auto ret_mmsg = client.alloc_region(msg.mmsg.size);
  CtrlMsg ret_msg{
      .op = CtrlOpCode::ALLOC, .ret = ret_mmsg.first, .mmsg = ret_mmsg.second};
  client_iter->second.cq.send(&ret_msg, sizeof(ret_msg));

  return 0;
}

int Daemon::do_free(const CtrlMsg &msg) {
  auto client_iter = _clients.find(msg.id);
  if (client_iter == _clients.cend()) {
    /* TODO: same as in do_disconnect */
    LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }

  uint64_t region_id = msg.mmsg.region_id;
  size_t region_size = msg.mmsg.size;
  const std::string chunkname = utils::get_region_name(msg.id, region_id);
  auto &client = client_iter->second;

  auto ret_mmsg = client.free_region(region_id);
  CtrlMsg ack{
      .op = CtrlOpCode::FREE, .ret = ret_mmsg.first, .mmsg = ret_mmsg.second};
  client_iter->second.cq.send(&ack, sizeof(ack));

  return 0;
}

void Daemon::serve() {
  LOG(kInfo) << "Daemon starts listening...";

  while (true) {
    CtrlMsg msg;
    size_t recvd_size;
    unsigned int prio;

    _ctrlq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
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
    case FREE:
      do_free(msg);
      break;
    default:
      LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }
}

static Daemon *get_daemon() {
  static std::mutex mtx_;
  static std::shared_ptr<Daemon> daemon_;
  if (daemon_)
    return daemon_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (daemon_)
    return daemon_.get();
  daemon_ = std::make_shared<Daemon>();
  return daemon_.get();
}

void signalHandler(int signum) {
  // Let the process exit normally so that daemon_ can be naturally destroyed.
  exit(signum);
}
} // namespace cachebank

int main(int argc, char *argv[]) {
  signal(SIGINT, cachebank::signalHandler);

  auto daemon = cachebank::get_daemon();
  daemon->serve();

  return 0;
}
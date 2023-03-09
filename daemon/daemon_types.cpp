#include <boost/interprocess/exceptions.hpp>
#include <chrono>
#include <fstream>
#include <memory>
#include <mutex>
#include <thread>

#include "inc/daemon_types.hpp"
#include "logging.hpp"
#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {

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
bool Client::alloc_region_(bool overcommit) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;

  if (overcommit || region_cnt_ < region_limit_) {
    int64_t region_id = new_region_id_();
    const auto rwmode = boost::interprocess::read_write;
    const std::string region_name = utils::get_region_name(id, region_id);
    boost::interprocess::permissions perms;
    perms.set_unrestricted();

    if (regions.find(region_id) == regions.cend()) {
      int64_t actual_size;
      auto region = std::make_shared<SharedMemObj>(
          boost::interprocess::create_only, region_name.c_str(), rwmode, perms);
      regions[region_id] = region;
      region_cnt_++;

      region->truncate(kRegionSize);
      region->get_size(actual_size);

      ret = CtrlRetCode::MEM_SUCC;
      mm.region_id = region_id;
      mm.size = actual_size;
    } else {
      /* region has already existed */
      MIDAS_LOG(kError) << "Client " << id << " has already allocated region "
                        << region_id;

      ret = CtrlRetCode::MEM_FAIL;
    }
  }

  CtrlMsg ret_msg{.op = CtrlOpCode::ALLOC, .ret = ret, .mmsg = mm};
  cq.send(&ret_msg, sizeof(ret_msg));
  return ret == CtrlRetCode::MEM_SUCC;
}

bool Client::free_region(int64_t region_id) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;
  int64_t actual_size;

  auto region_iter = regions.find(region_id);
  if (region_iter != regions.end()) {
    /* Successfully find the region to be freed */
    region_iter->second->get_size(actual_size);
    regions.erase(region_id);
    SharedMemObj::remove(utils::get_region_name(id, region_id).c_str());
    region_cnt_--;

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    MIDAS_LOG(kError) << "Client " << id << " doesn't have region "
                      << region_id;
    ret = CtrlRetCode::MEM_FAIL;
  }

  CtrlMsg ack{.op = CtrlOpCode::FREE, .ret = ret, .mmsg = mm};
  cq.send(&ack, sizeof(ack));
  return ret == CtrlRetCode::MEM_SUCC;
}

void Client::update_limit(uint64_t region_limit) {
  if (region_limit == region_limit_)
    return;
  region_limit_ = region_limit;

  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::UPDLIMIT,
              .ret = CtrlRetCode::MEM_FAIL,
              .mmsg{.size = region_limit_}};
  txqp.send(&msg, sizeof(msg));
  CtrlMsg ack;
  txqp.recv(&ack, sizeof(ack));

  if (ack.ret != CtrlRetCode::MEM_SUCC)
    MIDAS_LOG(kError) << ack.ret;
  // assert(ack.ret == CtrlRetCode::MEM_SUCC);
  // TODO: parse ack and react correspondingly
}

bool Client::profile_stats() {
  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::PROF_STATS};
  txqp.send(&msg, sizeof(msg));
  StatsMsg statsmsg;
  int ret = txqp.timed_recv(&statsmsg, sizeof(statsmsg), kProfInterval);
  if (ret != 0) {
    MIDAS_LOG(kInfo) << "Client " << id << " is dead!";
    return false;
  }
  double perf_gain = 0.;
  if (statsmsg.hits) {
    MIDAS_LOG(kError) << "Client " << id << " " << statsmsg.hits << " "
                      << statsmsg.misses << " " << statsmsg.miss_penalty << " "
                      << statsmsg.vhits;
    perf_gain = statsmsg.miss_penalty * statsmsg.vhits;
  }
  // update historical stats
  stats.hits = statsmsg.hits;
  stats.misses = statsmsg.misses;
  stats.penalty = statsmsg.miss_penalty;
  stats.vhits = statsmsg.vhits;
  stats.perf_gain = perf_gain;
  return true;
}

void Client::destroy() {
  for (const auto &kv : regions) {
    const std::string name = utils::get_region_name(id, kv.first);
    SharedMemObj::remove(name.c_str());
  }
}

/** Daemon */
Daemon::Daemon(const std::string cfg_file, const std::string ctrlq_name)
    : cfg_file_(cfg_file), ctrlq_name_(utils::get_rq_name(ctrlq_name, true)),
      region_cnt_(0), region_limit_(kRegionLimit),
      mem_limit_(kRegionLimit * kRegionSize), terminated_(false) {
  MsgQueue::remove(ctrlq_name_.c_str());
  boost::interprocess::permissions perms;
  perms.set_unrestricted();
  ctrlq_ = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                      ctrlq_name_.c_str(), kDaemonQDepth,
                                      kMaxMsgSize, perms);

  monitor_ = std::make_shared<std::thread>([&] { monitor(); });
  profiler_ = std::make_shared<std::thread>([&] { rebalancer(); });
}

Daemon::~Daemon() {
  terminated_ = true;
  if (monitor_)
    monitor_->join();
  if (profiler_)
    profiler_->join();
  clients_.clear();
  MsgQueue::remove(ctrlq_name_.c_str());
}

int Daemon::do_connect(const CtrlMsg &msg) {
  try {
    std::unique_lock<std::mutex> ul(mtx_);
    if (clients_.find(msg.id) != clients_.cend()) {
      MIDAS_LOG(kError) << "Client " << msg.id << " connected twice!";
      /* TODO: this might be some stale client. Probably we could try to
       * send the ack back */
      return -1;
    }
    ul.unlock();
    auto client =
        std::make_unique<Client>(msg.id, mem_limit_ / kRegionSize);
    client->connect();
    client->update_limit(mem_limit_ / kRegionSize); // init client-side mem_limit
    MIDAS_LOG(kInfo) << "Client " << msg.id << " connected.";
    ul.lock();
    clients_.insert(std::make_pair(msg.id, std::move(client)));
    ul.unlock();
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_disconnect(const CtrlMsg &msg) {
  try {
    std::unique_lock<std::mutex> ul(mtx_);
    auto client_iter = clients_.find(msg.id);
    if (client_iter == clients_.cend()) {
      /* TODO: this might be some unregistered client. Probably we could try to
       * connect to it and send the ack back */
      MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
      return -1;
    }
    auto client = std::move(client_iter->second);
    clients_.erase(msg.id);
    ul.unlock();
    client->disconnect();
    MIDAS_LOG(kInfo) << "Client " << msg.id << " disconnected!";
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }

  return 0;
}

int Daemon::do_alloc(const CtrlMsg &msg) {
  assert(msg.mmsg.size == kRegionSize);
  std::unique_lock<std::mutex> ul(mtx_);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  auto &client = client_iter->second;
  assert(msg.id == client->id);
  client->alloc_region();

  return 0;
}

int Daemon::do_overcommit(const CtrlMsg &msg) {
  assert(msg.mmsg.size == kRegionSize);
  std::unique_lock<std::mutex> ul(mtx_);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  auto &client = client_iter->second;
  assert(msg.id == client->id);
  client->overcommit_region();

  return 0;
}

int Daemon::do_free(const CtrlMsg &msg) {
  auto client_iter = clients_.find(msg.id);
  std::unique_lock<std::mutex> ul(mtx_);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  uint64_t region_id = msg.mmsg.region_id;
  size_t region_size = msg.mmsg.size;
  auto &client = client_iter->second;
  client->free_region(region_id);

  return 0;
}

int Daemon::do_update_limit_req(const CtrlMsg &msg) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  size_t upd_mem_limit = std::min(msg.mmsg.size, mem_limit_);
  auto &client = client_iter->second;
  client->update_limit(upd_mem_limit / kRegionSize);

  return 0;
}

void Daemon::monitor() {
  while (!terminated_) {
    uint64_t upd_mem_limit;
    std::ifstream cfg(cfg_file_);
    if (!cfg.is_open()) {
      MIDAS_LOG(kError) << "open " << cfg_file_ << " failed!";
      return;
    }
    cfg >> upd_mem_limit;
    cfg.close();

    if (mem_limit_ != upd_mem_limit) {
      MIDAS_LOG(kError) << mem_limit_ << " != " << upd_mem_limit;
      mem_limit_ = upd_mem_limit;
      region_limit_ = mem_limit_ / kRegionSize;
      // TODO: update limit of each client
      // std::unique_lock<std::mutex> ul(mtx_);
      // for (auto &[_, client] : clients_) {
      //   client->update_limit(mem_limit_);
      // }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

void Daemon::rebalancer() {
  while (!terminated_) {
    std::this_thread::sleep_for(std::chrono::seconds(kProfInterval));
    std::vector<uint64_t> dead_clients;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      bool alive = client->profile_stats();
      if (!alive) {
        dead_clients.emplace_back(client->id);
      }
      auto perf_gain = client->stats.perf_gain;
      if (perf_gain < 1.0)
        continue;
      if (perf_gain < 500.0) {
        client->update_limit(std::max(client->region_limit_ - 4, 100ul));
      } else if (perf_gain > 4000.0) {
        client->update_limit((client->region_limit_ + 4));
      }
    }
    for (const auto &cid : dead_clients) {
      clients_.erase(cid);
    }
    dead_clients.clear();
  }
}

void Daemon::serve() {
  MIDAS_LOG(kInfo) << "Daemon starts listening...";

  while (!terminated_) {
    CtrlMsg msg;
    size_t recvd_size;
    unsigned int prio;

    ctrlq_->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      break;
    }
    MIDAS_LOG(kDebug) << "Daemon recved msg " << msg.op;
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
    case UPDLIMIT_REQ:
      do_update_limit_req(msg);
      break;
    default:
      MIDAS_LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }
}

} // namespace midas
#include <atomic>
#include <boost/interprocess/exceptions.hpp>
#include <chrono>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <thread>

#include "inc/daemon_types.hpp"
#include "logging.hpp"
#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {
constexpr static float kPerfZeroThresh = 0.1;

/** Client */
Client::Client(Daemon *daemon, uint64_t id_, uint64_t region_limit)
    : daemon_(daemon), status(ClientStatusCode::INIT), id(id_), region_cnt_(0),
      region_limit_(region_limit),
      cq(utils::get_ackq_name(kNameCtrlQ, id), false),
      txqp(std::to_string(id), false) {
  daemon_->charge(region_limit_);
}

Client::~Client() {
  daemon_->uncharge(region_limit_);
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
  daemon_->charge(region_limit - region_limit_);
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
  if (statsmsg.hits) {
    MIDAS_LOG(kError) << "Client " << id << " " << statsmsg.hits << " "
                      << statsmsg.misses << " " << statsmsg.miss_penalty << " "
                      << statsmsg.vhits;
  }
  // update historical stats
  stats.hits = stats.hits * KProfWDecay + statsmsg.hits * (1 - KProfWDecay);
  stats.misses =
      stats.misses * KProfWDecay + statsmsg.misses * (1 - KProfWDecay);
  stats.penalty =
      stats.penalty * KProfWDecay + statsmsg.miss_penalty * (1 - KProfWDecay);
  stats.vhits = stats.vhits * KProfWDecay + statsmsg.vhits * (1 - KProfWDecay);
  stats.perf_gain = stats.penalty * stats.vhits;
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
      region_cnt_(0), region_limit_(kMaxRegions), terminated_(false),
      status_(MemStatus::NORMAL) {
  MsgQueue::remove(ctrlq_name_.c_str());
  boost::interprocess::permissions perms;
  perms.set_unrestricted();
  ctrlq_ = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                      ctrlq_name_.c_str(), kDaemonQDepth,
                                      kMaxMsgSize, perms);

  monitor_ = std::make_shared<std::thread>([&] { monitor(); });
  profiler_ = std::make_shared<std::thread>([&] { profiler(); });
  rebalancer_ = std::make_shared<std::thread>([&] { rebalancer(); });
}

Daemon::~Daemon() {
  terminated_ = true;
  {
    std::unique_lock<std::mutex> ul(rbl_mtx_);
    rbl_cv_.notify_all();
  }
  if (monitor_)
    monitor_->join();
  if (profiler_)
    profiler_->join();
  if (rebalancer_)
    rebalancer_->join();
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
    auto client = std::make_unique<Client>(this, msg.id, kInitRegions);
    client->connect();
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
  size_t upd_region_lim = std::min(msg.mmsg.size / kRegionSize, region_limit_);
  auto &client = client_iter->second;
  if (upd_region_lim != client->region_limit_) {
    client->update_limit(upd_region_lim);
  }

  return 0;
}

void Daemon::charge(int64_t nr_regions) {
  region_cnt_ += nr_regions;
  if (region_cnt_ > region_limit_) {
    std::unique_lock<std::mutex> ul(rbl_mtx_);
    status_ = MemStatus::NEED_SHRINK;
    rbl_cv_.notify_one();
  }
}

void Daemon::uncharge(int64_t nr_regions) {
  region_cnt_ -= nr_regions;
  if (region_cnt_ < region_limit_) {
    std::unique_lock<std::mutex> ul(rbl_mtx_);
    status_ = MemStatus::NEED_EXPAND;
    rbl_cv_.notify_one();
  }
}

void Daemon::monitor() {
  while (!terminated_) {
    std::ifstream cfg(cfg_file_);
    if (!cfg.is_open()) {
      MIDAS_LOG(kError) << "open " << cfg_file_ << " failed!";
      return;
    }

    uint64_t upd_mem_limit;
    cfg >> upd_mem_limit;
    cfg.close();
    uint64_t upd_region_limit = upd_mem_limit / kRegionSize;
    if (region_limit_ != upd_region_limit) {
      MIDAS_LOG(kError) << region_limit_ << " != " << upd_region_limit;
      region_limit_ = upd_region_limit;
      if (region_cnt_ > region_limit_) { // invoke rebalancer
        std::unique_lock<std::mutex> ul(rbl_mtx_);
        status_ = MemStatus::NEED_SHRINK;
        rbl_cv_.notify_one();
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

void Daemon::profiler() {
  while (!terminated_) {
    std::this_thread::sleep_for(std::chrono::seconds(kProfInterval));
    std::atomic_int_fast32_t nr_active_clients{0};
    std::mutex dead_mtx;
    std::vector<uint64_t> dead_clients;
    std::vector<std::future<void>> futures;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      auto future = std::async(std::launch::async, [&, &client = client] {
        bool alive = client->profile_stats();
        if (!alive) {
          std::unique_lock<std::mutex> ul(dead_mtx);
          dead_clients.emplace_back(client->id);
          return;
        }
        if (client->stats.perf_gain > kPerfZeroThresh)
          nr_active_clients++;
      });
      futures.emplace_back(std::move(future));
    }
    for (auto &f : futures)
      f.get();
    for (const auto &cid : dead_clients)
      clients_.erase(cid);
    dead_clients.clear();

    // invoke rebalancer
    if (region_cnt_ < region_limit_ && nr_active_clients > 0) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_EXPAND;
      rbl_cv_.notify_one();
    } else if (region_cnt_ == region_limit_) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_REBALANCE;
      rbl_cv_.notify_one();
    }
  }
}

void Daemon::rebalancer() {
  while (!terminated_) {
    {
      std::unique_lock<std::mutex> lk(rbl_mtx_);
      rbl_cv_.wait(
          lk, [this] { return terminated_ || status_ != MemStatus::NORMAL; });
    }
    if (terminated_)
      break;
    switch (status_) {
    case MemStatus::NEED_SHRINK:
      on_mem_shrink();
      break;
    case MemStatus::NEED_EXPAND:
      on_mem_expand();
      break;
    case MemStatus::NEED_REBALANCE:
      on_mem_rebalance();
      break;
    default:
      MIDAS_LOG(kError) << "Memory rebalancer is waken up for unknown reason";
    }
    status_ = MemStatus::NORMAL;
  }
}

void Daemon::on_mem_shrink() {
  while (region_cnt_ > region_limit_) {
    int64_t nr_to_reclaim = region_cnt_ - region_limit_;
    double nr_reclaim_ratio = static_cast<double>(nr_to_reclaim) / region_cnt_;

    std::vector<Client *> inactive_clients;
    std::vector<Client *> active_clients;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->stats.perf_gain < kPerfZeroThresh)
        inactive_clients.emplace_back(client.get());
      else
        active_clients.emplace_back(client.get());
    }
    ul.unlock();
    for (auto client : inactive_clients) {
      auto old_limit = client->region_limit_;
      int64_t nr_reclaimed = std::ceil(nr_reclaim_ratio * old_limit);
      uint64_t new_limit = old_limit - nr_reclaimed;
      client->update_limit(new_limit);
      nr_to_reclaim -= nr_reclaimed;
      MIDAS_LOG_PRINTF(kInfo, "Reclaimed client %lu %ld regions, %lu -> %lu\n",
                       client->id, nr_reclaimed, old_limit, new_limit);
    }
    if (nr_to_reclaim <= 0)
      break;
    double total_gain = 0.0;
    for (auto client : active_clients)
      total_gain += client->stats.perf_gain;
    for (auto client : active_clients) {
      auto gain = client->stats.perf_gain;
      auto old_limit = client->region_limit_;
      int64_t nr_reclaimed = std::ceil(gain / total_gain * nr_to_reclaim);
      auto new_limit = old_limit - nr_reclaimed;
      client->update_limit(new_limit);
      nr_to_reclaim -= nr_reclaimed;
      MIDAS_LOG_PRINTF(kInfo, "Reclaimed client %lu %ld regions, %lu -> %lu\n",
                       client->id, nr_reclaimed, old_limit, new_limit);
    }
  }
  MIDAS_LOG(kInfo) << "Memory shrink done! Total regions: " << region_cnt_
                   << "/" << region_limit_;
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, client] : clients_) {
    MIDAS_LOG(kInfo) << "Client " << client->id
                     << " regions: " << client->region_cnt_ << "/"
                     << client->region_limit_;
  }
}

void Daemon::on_mem_expand() {
  int64_t nr_to_grant = region_limit_ - region_cnt_;
  while (nr_to_grant > 0) {
    double nr_grant_ratio = static_cast<double>(nr_to_grant) / region_limit_;

    std::vector<Client *> active_clients;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->stats.perf_gain > kPerfZeroThresh)
        active_clients.emplace_back(client.get());
    }
    ul.unlock();
    if (active_clients.empty())
      return;
    double total_gain = 0.0;
    for (auto client : active_clients)
      total_gain += client->stats.perf_gain;
    for (auto client : active_clients) {
      auto gain = client->stats.perf_gain;
      auto old_limit = client->region_limit_;
      int64_t nr_granted = std::floor(gain / total_gain * nr_to_grant);
      auto new_limit = old_limit + nr_granted;
      client->update_limit(new_limit);
      nr_to_grant -= nr_granted;
      MIDAS_LOG_PRINTF(kInfo, "Grant client %lu %ld regions, %lu -> %lu\n",
                       client->id, nr_granted, old_limit, new_limit);
    }
  }
  MIDAS_LOG(kInfo) << "Memory expansion done! Total regions: " << region_cnt_
                   << "/" << region_limit_;
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, client] : clients_) {
    MIDAS_LOG(kInfo) << "Client " << client->id
                     << " regions: " << client->region_cnt_ << "/"
                     << client->region_limit_;
  }
}

void Daemon::on_mem_rebalance() {
  std::vector<Client *> clients;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_)
      clients.emplace_back(client.get());
  }
  std::sort(clients.begin(), clients.end(), [](Client *c1, Client *c2) {
    return c1->stats.perf_gain < c2->stats.perf_gain;
  });

  if (clients.size() <= 1)
    return;

  Client *winner = nullptr;
  while (!clients.empty()) {
    winner = clients.back();
    clients.pop_back();
    if (winner->stats.perf_gain > kPerfZeroThresh)
      break;
  }
  if (!winner)
    return;
  MIDAS_LOG(kError) << "Winner " << winner->id << " : "
                    << winner->stats.perf_gain;
  uint64_t nr_reclaimed = 0;
  for (auto client : clients) {
    MIDAS_LOG(kError) << client->stats.perf_gain;
    auto nr_to_reclaim = std::min(client->region_limit_, 8ul);
    client->update_limit(client->region_limit_ - nr_to_reclaim);
    nr_reclaimed += nr_to_reclaim;
  }
  winner->update_limit(winner->region_limit_ + nr_reclaimed);
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
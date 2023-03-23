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
constexpr static bool kEnableProfiler = true;
constexpr static bool kEnableRebalancer = true;
/** Profiler related */
constexpr static uint32_t kMonitorInteral = 1; // in seconds
constexpr static uint32_t kProfInterval = 5;   // in seconds
constexpr static float KProfWDecay = 0.3;
/** Rebalancer related */
constexpr static float kExpandThresh = 0.5;
constexpr static float kExpandFactor = 0.5;
/** Server related */
constexpr static uint32_t kAliveTimeout = 3;   // in seconds
constexpr static uint32_t kReclaimTimeout = 5; // in seconds
constexpr static uint32_t kServeTimeout = 1;   // in seconds

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
  CtrlMsg ack{.op = CtrlOpCode::CONNECT,
              .ret = CtrlRetCode::CONN_SUCC,
              .mmsg{.size = region_limit_ * kRegionSize}};
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

bool Client::update_limit(uint64_t region_limit) {
  if (region_limit == region_limit_)
    return true;
  daemon_->charge(region_limit - region_limit_);
  region_limit_ = region_limit;

  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::UPDLIMIT,
              .ret = CtrlRetCode::MEM_FAIL,
              .mmsg{.size = region_limit_}};
  txqp.send(&msg, sizeof(msg));
  CtrlMsg ack;
  if (txqp.timed_recv(&ack, sizeof(ack), kReclaimTimeout) != 0) {
    MIDAS_LOG(kError) << "Client " << id << " reclamation timed out!";
    return false;
  }
  assert(ack.mmsg.size == region_cnt_);
  if (ack.ret != CtrlRetCode::MEM_SUCC) {
    MIDAS_LOG(kError) << "Client " << id << " failed to reclaim" << region_cnt_
                      << "/" << region_limit_;
  }
  return true;
}

bool Client::profile_stats() {
  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::PROF_STATS};
  txqp.send(&msg, sizeof(msg));
  StatsMsg statsmsg;
  int ret = txqp.timed_recv(&statsmsg, sizeof(statsmsg), kAliveTimeout);
  if (ret != 0) {
    MIDAS_LOG(kInfo) << "Client " << id << " is dead!";
    return false;
  }
  if (statsmsg.hits || statsmsg.misses) {
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
      ctrlq_(ctrlq_name_, true, kDaemonQDepth, kMaxMsgSize), region_cnt_(0),
      region_limit_(kMaxRegions), terminated_(false),
      status_(MemStatus::NORMAL), monitor_(nullptr), profiler_(nullptr),
      rebalancer_(nullptr) {
  monitor_ = std::make_shared<std::thread>([&] { monitor(); });
  if (kEnableProfiler)
    profiler_ = std::make_shared<std::thread>([&] { profiler(); });
  if (kEnableProfiler && kEnableRebalancer)
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
    auto client = std::make_shared<Client>(this, msg.id, kInitRegions);
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
  auto upd_region_lim = std::min(msg.mmsg.size / kRegionSize, region_limit_);
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

    std::this_thread::sleep_for(std::chrono::seconds(kMonitorInteral));
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
    ul.unlock();
    for (auto &f : futures)
      f.get();
    ul.lock();
    for (const auto &cid : dead_clients)
      clients_.erase(cid);
    dead_clients.clear();
    ul.unlock();

    // invoke rebalancer
    if (region_cnt_ < region_limit_ && nr_active_clients > 0) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_EXPAND;
      rbl_cv_.notify_one();
    } else if (region_cnt_ == region_limit_) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_REBALANCE;
      rbl_cv_.notify_one();
    } else if (region_cnt_ > region_limit_) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_SHRINK;
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

    std::vector<std::shared_ptr<Client>> inactive_clients;
    std::vector<std::shared_ptr<Client>> active_clients;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->stats.perf_gain < kPerfZeroThresh)
        inactive_clients.emplace_back(client);
      else
        active_clients.emplace_back(client);
    }
    ul.unlock();
    for (auto client : inactive_clients) {
      int64_t old_limit = client->region_limit_;
      int64_t nr_reclaimed = std::ceil(nr_reclaim_ratio * old_limit);
      int64_t new_limit = std::max(1l, old_limit - nr_reclaimed);
      client->update_limit(new_limit);
      nr_to_reclaim -= nr_reclaimed;
      MIDAS_LOG_PRINTF(kInfo, "Reclaimed client %lu %ld regions, %lu -> %lu\n",
                       client->id, nr_reclaimed, old_limit, new_limit);
    }
    if (region_cnt_ <= region_limit_)
      break;
    double total_gain = 0.0;
    for (auto client : active_clients)
      total_gain += client->stats.perf_gain;
    for (auto client : active_clients) {
      auto gain = client->stats.perf_gain;
      int64_t old_limit = client->region_limit_;
      int64_t nr_reclaimed = std::ceil(gain / total_gain * nr_to_reclaim);
      int64_t new_limit = std::max(1l, old_limit - nr_reclaimed);
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
  bool expanded = false;
  int64_t nr_to_grant = region_limit_ - region_cnt_;
  if (nr_to_grant <= 0)
    return;
  double nr_grant_ratio = static_cast<double>(nr_to_grant) / region_limit_;

  std::vector<std::shared_ptr<Client>> active_clients;
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, client] : clients_) {
    if (client->stats.perf_gain > kPerfZeroThresh)
      active_clients.emplace_back(client);
  }
  ul.unlock();
  if (active_clients.empty())
    return;
  double total_gain = 0.0;
  for (auto client : active_clients) {
    if (client->region_cnt_ < client->region_limit_ * kExpandThresh)
      continue;
    total_gain += client->stats.perf_gain;
  }
  if (total_gain < kPerfZeroThresh)
    return;
  for (auto client : active_clients) {
    if (client->region_cnt_ < client->region_limit_ * kExpandThresh)
      continue;
    auto gain = client->stats.perf_gain;
    auto old_limit = client->region_limit_;
    int64_t nr_granted = std::min<int64_t>(
        std::min<int64_t>(std::ceil(gain / total_gain * nr_to_grant),
                          std::ceil(old_limit * (1 - kExpandFactor))),
        nr_to_grant);
    auto new_limit = old_limit + nr_granted;
    client->update_limit(new_limit);
    nr_to_grant -= nr_granted;
    expanded = true;
    MIDAS_LOG_PRINTF(kInfo, "Grant client %lu %ld regions, %lu -> %lu\n",
                     client->id, nr_granted, old_limit, new_limit);
  }
  if (expanded) {
    MIDAS_LOG(kInfo) << "Memory expansion done! Total regions: " << region_cnt_
                     << "/" << region_limit_;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      MIDAS_LOG(kInfo) << "Client " << client->id
                       << " regions: " << client->region_cnt_ << "/"
                       << client->region_limit_;
    }
  }
}

void Daemon::on_mem_rebalance() {
  constexpr static uint64_t kMaxStepSize = 64;
  static uint64_t kStepSize = 4ul;
  static uint64_t prev_winner = -1ul;

  std::vector<std::shared_ptr<Client>> clients;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_)
      clients.emplace_back(client);
  }
  std::sort(clients.begin(), clients.end(),
            [](std::shared_ptr<Client> c1, std::shared_ptr<Client> c2) {
              return c1->stats.perf_gain < c2->stats.perf_gain;
            });

  if (clients.size() <= 1)
    return;

  std::shared_ptr<Client> winner = nullptr;
  while (!clients.empty()) {
    winner = clients.back();
    clients.pop_back();
    if (winner->stats.perf_gain > kPerfZeroThresh ||
        winner->region_cnt_ >= winner->region_limit_ * kExpandThresh)
      break;
  }
  if (!winner)
    return;
  if (winner->id == prev_winner) {
    kStepSize = std::min(kMaxStepSize, kStepSize * 2);
  } else {
    prev_winner = winner->id;
  }
  MIDAS_LOG(kInfo) << "Winner " << winner->id
                   << ", perf gain: " << winner->stats.perf_gain;
  uint64_t nr_reclaimed = 0;
  for (auto client : clients) {
    // each client must have at least 1 region
    auto nr_to_reclaim = std::min(client->region_limit_ - 1, kStepSize);
    client->update_limit(client->region_limit_ - nr_to_reclaim);
    nr_reclaimed += nr_to_reclaim;
  }
  winner->update_limit(winner->region_limit_ + nr_reclaimed);
  if (nr_reclaimed) {
    MIDAS_LOG(kInfo) << "Memory rebalance done! Total regions: " << region_cnt_
                     << "/" << region_limit_;
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      MIDAS_LOG(kInfo) << "Client " << client->id
                       << " regions: " << client->region_cnt_ << "/"
                       << client->region_limit_;
    }
  }
}

void Daemon::serve() {
  MIDAS_LOG(kInfo) << "Daemon starts listening...";

  while (!terminated_) {
    CtrlMsg msg;
    if (ctrlq_.timed_recv(&msg, sizeof(CtrlMsg), kServeTimeout) != 0)
      continue;

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

  MIDAS_LOG(kInfo) << "Daemon stopped to serve...";
}

} // namespace midas
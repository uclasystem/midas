#include <atomic>
#include <boost/interprocess/exceptions.hpp>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <sys/sysinfo.h>
#include <thread>

#include "inc/daemon_types.hpp"
#include "logging.hpp"
#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {
/** Global control flags */
/* by default set to "false" and Midas deamon will automatically detect system
 * avail memory. Set it to "true" to simulate memory pressure by arbitrarily
 * update the memory cfg file. */
constexpr static bool kEnableMemPressureSimu = true;
constexpr static bool kEnableDynamicRebalance = true;
constexpr static Daemon::Policy kDefaultPolicy = Daemon::Policy::Midas;
// WARNING: two flags below should always be enabled to adapt clients' memory
// usage to the amount of server's idle memory
constexpr static bool kEnableProfiler = true;
constexpr static bool kEnableRebalancer = true;
/** Monitor related */
constexpr static uint32_t kMonitorInteral = 1; // in seconds
/** Profiler related */
constexpr static float kPerfZeroThresh = 1;
constexpr static uint32_t kProfInterval = 5; // in seconds
constexpr static float KProfWDecay = 0.3;
/** Rebalancer related */
constexpr static float kExpandFactor = 0.5;
constexpr static uint32_t kMaxExpandThresh = 128;
constexpr static int32_t kWarmupRounds = 2;
/** Server related */
constexpr static uint32_t kAliveTimeout = 3;   // in seconds
constexpr static uint32_t kReclaimTimeout = 5; // in seconds
constexpr static uint32_t kServeTimeout = 1;   // in seconds

/** Client */
Client::Client(Daemon *daemon, uint64_t id_, uint64_t region_limit)
    : daemon_(daemon), status(ClientStatusCode::INIT), id(id_), region_cnt_(0),
      region_limit_(region_limit), weight_(1), warmup_ttl_(0),
      lat_critical_(false), cq(utils::get_ackq_name(kNameCtrlQ, id), false),
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
  status = ClientStatusCode::CONNECTED;
}

void Client::disconnect() {
  status = ClientStatusCode::DISCONNECTED;
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

void Client::set_weight(float weight) {
  weight_ = weight;
  MIDAS_LOG(kInfo) << "Client " << id << " set weight to " << weight_;
}

void Client::set_lat_critical(bool value) {
  lat_critical_ = value;
  MIDAS_LOG(kInfo) << "Client " << id << " set lat_critical to "
                   << lat_critical_;
}

bool Client::update_limit(uint64_t new_limit) {
  if (new_limit + kForceReclaimThresh < region_limit_)
    return force_reclaim(new_limit);
  if (new_limit == region_limit_)
    return true;
  daemon_->charge(new_limit - region_limit_);
  region_limit_ = new_limit;

  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::UPDLIMIT,
              .ret = CtrlRetCode::MEM_FAIL,
              .mmsg{.size = region_limit_}};
  txqp.send(&msg, sizeof(msg));
  CtrlMsg ack;
  if (txqp.timed_recv(&ack, sizeof(ack), kReclaimTimeout) != 0) {
    MIDAS_LOG(kError) << "Client " << id << " timed out!";
    return false;
  }
  if (ack.mmsg.size != region_cnt_) {
    MIDAS_LOG(kError) << ack.mmsg.size << " != " << region_cnt_;
  }
  // assert(ack.mmsg.size <= region_cnt_);
  if (ack.ret != CtrlRetCode::MEM_SUCC) {
    MIDAS_LOG(kError) << "Client " << id << " failed to reclaim " << region_cnt_
                      << "/" << region_limit_;
  }
  return true;
}

bool Client::force_reclaim(uint64_t new_limit) {
  if (new_limit == region_limit_)
    return true;
  daemon_->charge(new_limit - region_limit_);
  region_limit_ = new_limit;

  std::unique_lock<std::mutex> ul(tx_mtx);
  CtrlMsg msg{.op = CtrlOpCode::FORCE_RECLAIM,
              .ret = CtrlRetCode::MEM_FAIL,
              .mmsg{.size = region_limit_}};
  txqp.send(&msg, sizeof(msg));
  CtrlMsg ack;
  if (txqp.timed_recv(&ack, sizeof(ack), kReclaimTimeout) != 0) {
    MIDAS_LOG(kError) << "Client " << id << " timed out!";
    return false;
  }
  // assert(ack.mmsg.size <= region_cnt_);
  if (ack.ret != CtrlRetCode::MEM_SUCC) {
    MIDAS_LOG(kError) << "Client " << id << " failed to force reclaim "
                      << region_cnt_ << "/" << region_limit_;
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
    MIDAS_LOG(kWarning) << "Client " << id << " is dead!";
    return false;
  }
  if (statsmsg.hits || statsmsg.misses) {
    MIDAS_LOG(kDebug) << "Client " << id << " " << statsmsg.hits << " "
                      << statsmsg.misses << " " << statsmsg.miss_penalty << " "
                      << statsmsg.vhits;
  }
  // update historical stats
  stats.hits = stats.hits * KProfWDecay + statsmsg.hits * (1 - KProfWDecay);
  stats.misses =
      stats.misses * KProfWDecay + statsmsg.misses * (1 - KProfWDecay);
  stats.vhits = stats.vhits * KProfWDecay + statsmsg.vhits * (1 - KProfWDecay);
  stats.penalty =
      stats.penalty * KProfWDecay + statsmsg.miss_penalty * (1 - KProfWDecay);
  stats.perf_gain = weight_ * stats.penalty * stats.vhits;
  stats.headroom = std::max<int32_t>(1, statsmsg.headroom);
  return true;
}

void Client::destroy() {
  for (const auto &kv : regions) {
    const std::string name = utils::get_region_name(id, kv.first);
    SharedMemObj::remove(name.c_str());
  }
}

bool Client::almost_full() noexcept {
  auto threshold = std::max<int64_t>(kMaxExpandThresh, 4 * stats.headroom);
  return region_cnt_ >= region_limit_ - threshold;
}

/** Daemon */
Daemon::Daemon(const std::string ctrlq_name)
    : ctrlq_name_(utils::get_rq_name(ctrlq_name, true)),
      ctrlq_(ctrlq_name_, true, kDaemonQDepth, kMaxMsgSize), region_cnt_(0),
      region_limit_(kMaxRegions), terminated_(false),
      status_(MemStatus::NORMAL), policy(kDefaultPolicy), monitor_(nullptr),
      profiler_(nullptr), rebalancer_(nullptr) {
  monitor_ = std::make_shared<std::thread>([&] { monitor(); });
  if (kEnableProfiler)
    profiler_ = std::make_shared<std::thread>([&] { profiler(); });
  if (kEnableRebalancer)
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
    bool succ = client->update_limit(upd_region_lim);
  }

  return 0;
}

int Daemon::do_set_weight(const CtrlMsg &msg) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  float weight = msg.mmsg.weight;
  auto &client = client_iter->second;
  client->set_weight(weight);
  return 0;
}

int Daemon::do_set_lat_critical(const CtrlMsg &msg) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto client_iter = clients_.find(msg.id);
  if (client_iter == clients_.cend()) {
    /* TODO: same as in do_disconnect */
    MIDAS_LOG(kError) << "Client " << msg.id << " doesn't exist!";
    return -1;
  }
  ul.unlock();
  bool lat_critical = msg.mmsg.lat_critical;
  auto &client = client_iter->second;
  client->set_lat_critical(lat_critical);
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
    // monitor & update mem limit
    auto upd_mem_limit = kEnableMemPressureSimu ? utils::check_file_avail_mem()
                                                : utils::check_sys_avail_mem();
    uint64_t upd_region_limit = upd_mem_limit / kRegionSize;
    if (region_limit_ != upd_region_limit) {
      if (kEnableMemPressureSimu)
        MIDAS_LOG(kInfo) << region_limit_ << " != " << upd_region_limit;
      region_limit_ = upd_region_limit;
      if (region_cnt_ > region_limit_) { // invoke rebalancer
        std::unique_lock<std::mutex> ul(rbl_mtx_);
        status_ = MemStatus::NEED_SHRINK;
        rbl_cv_.notify_one();
      }
    }

    // monitor & update policy
    auto new_policy = utils::check_policy();
    if (new_policy >= Policy::Static && new_policy < Policy::NumPolicy &&
        new_policy != policy) {
      MIDAS_LOG(kError) << "Policy changed " << policy << " -> " << new_policy;
      policy = static_cast<Policy>(new_policy);
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
    if (region_cnt_ < region_limit_) {
      if (nr_active_clients > 0) {
        std::unique_lock<std::mutex> ul(rbl_mtx_);
        status_ = MemStatus::NEED_EXPAND;
        rbl_cv_.notify_one();
      }
    } else if (region_cnt_ == region_limit_) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_REBALANCE;
      rbl_cv_.notify_one();
    } else if (region_cnt_ > region_limit_) {
      std::unique_lock<std::mutex> ul(rbl_mtx_);
      status_ = MemStatus::NEED_SHRINK;
      rbl_cv_.notify_one();
    } else {
      MIDAS_ABORT("Impossible to reach here!");
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
      if (policy == Policy::Static) {
        /* do nothing */
      } else if (policy == Policy::Midas)
        on_mem_rebalance();
      else if (policy == Policy::CliffHanger)
        on_mem_rebalance_cliffhanger();
      else {
        on_mem_rebalance();
      }
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
      bool succ = client->update_limit(new_limit);
      nr_to_reclaim -= nr_reclaimed;
      MIDAS_LOG_PRINTF(kInfo, "Reclaimed client %lu %ld regions, %lu -> %lu\n",
                       client->id, nr_reclaimed, old_limit, new_limit);
    }
    if (region_cnt_ <= region_limit_)
      break;
    double total_reciprocal_gain = 0.0;
    for (auto client : active_clients)
      total_reciprocal_gain += 1.0 / client->stats.perf_gain;
    for (auto client : active_clients) {
      auto reciprocal_gain = 1.0 / client->stats.perf_gain;
      int64_t old_limit = client->region_limit_;
      int64_t nr_reclaimed = std::max<int64_t>(
          std::ceil(reciprocal_gain / total_reciprocal_gain * nr_to_reclaim),
          1);
      int64_t new_limit = std::max(1l, old_limit - nr_reclaimed);
      bool succ = client->update_limit(new_limit);
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
  if (policy == Policy::Static)
    return;

  bool expanded = false;
  int64_t nr_to_grant = region_limit_ - region_cnt_;
  if (nr_to_grant <= 0)
    return;

  std::vector<std::shared_ptr<Client>> active_clients;
  std::vector<std::shared_ptr<Client>> empty_clients;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->stats.perf_gain > kPerfZeroThresh && client->almost_full())
        active_clients.emplace_back(client);
      else if (client->region_limit_ <= 10)
        empty_clients.emplace_back(client);
    }
  }

  if (policy == Policy::ExpandOnly) {
    if (!empty_clients.empty()) { // re-enable an empty pool
      int64_t nr_per_client = nr_to_grant / empty_clients.size();
      nr_per_client = std::min<int64_t>(kMaxExpandThresh, nr_per_client);
      for (auto client : empty_clients) {
        nr_to_grant -= nr_per_client;
        auto old_limit = client->region_limit_;
        auto new_limit = old_limit + nr_per_client;
        client->update_limit(new_limit);
        expanded = true;
        MIDAS_LOG_PRINTF(kInfo, "Grant client %lu %ld regions, %lu -> %lu\n",
                         client->id, nr_per_client, old_limit, new_limit);
      }
    }

    int64_t nr_per_client = nr_to_grant / active_clients.size();
    double total_gain = 0.0;
    for (auto client : active_clients)
      total_gain += std::log2(client->stats.perf_gain);

    for (auto client : active_clients) {
      auto gain = std::log2(client->stats.perf_gain);
      int64_t nr_per_client = gain / total_gain * nr_to_grant;

      auto old_limit = client->region_limit_;
      auto new_limit = old_limit + nr_per_client;
      client->update_limit(client->region_limit_ + nr_per_client);
      expanded = true;
      MIDAS_LOG_PRINTF(kInfo, "Grant client %lu %ld regions, %lu -> %lu\n",
                      client->id, nr_per_client, old_limit, new_limit);
    }
  } else {
    if (active_clients.empty()) {
      if (!empty_clients.empty()) {
        // re-enable an empty pool
        int64_t nr_per_client = nr_to_grant / empty_clients.size();
        nr_per_client = std::min<int64_t>(kMaxExpandThresh, nr_per_client);
        for (auto client : empty_clients) {
          client->update_limit(client->region_limit_ + nr_per_client);
          expanded = true;
        }
      }
    } else { // expand soft memory grant among active clients
      double total_gain = 0.0;
      for (auto client : active_clients)
        total_gain += std::log2(client->stats.perf_gain);

      for (auto client : active_clients) {
        auto gain = std::log2(client->stats.perf_gain);
        auto old_limit = client->region_limit_;
        int64_t nr_granted = std::min<int64_t>(
            std::min<int64_t>(std::ceil(gain / total_gain * nr_to_grant),
                              std::ceil(old_limit * (1 - kExpandFactor))),
            nr_to_grant);
        nr_granted = std::min<int64_t>(nr_granted, kMaxExpandThresh);
        auto new_limit = old_limit + nr_granted;
        bool succ = client->update_limit(new_limit);
        nr_to_grant -= nr_granted;
        expanded = true;
        MIDAS_LOG_PRINTF(kInfo, "Grant client %lu %ld regions, %lu -> %lu\n",
                         client->id, nr_granted, old_limit, new_limit);
      }
    }
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
  constexpr static int64_t kMaxReclaim = 64;
  int64_t kMaxGrant = 128;
  if (!kEnableDynamicRebalance)
    return;

  std::vector<std::shared_ptr<Client>> idles;
  std::vector<std::shared_ptr<Client>> victims;
  std::vector<std::shared_ptr<Client>> candidates;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->region_limit_ <= 1)
        continue;
      if (client->warmup_ttl_ > 0) {
        client->warmup_ttl_--;
        continue;
      }

      if (client->stats.perf_gain <= kPerfZeroThresh) {
        if (client->lat_critical_ && client->almost_full())
          continue;
        idles.emplace_back(client);
      } else
        candidates.emplace_back(client);
    }
  }
  if (candidates.empty())
    return;
  if (idles.empty())
    kMaxGrant /= 4; // 32;

  // planning
  std::sort(candidates.begin(), candidates.end(),
            [](std::shared_ptr<Client> c1, std::shared_ptr<Client> c2) {
              return c1->stats.perf_gain > c2->stats.perf_gain;
            });
  float max_gain = std::log2(candidates.front()->stats.perf_gain);
  float min_gain = std::log2(candidates.back()->stats.perf_gain);
  std::vector<int64_t> plan_idle_reclaims(idles.size());
  std::vector<int64_t> plan_adjusts(candidates.size());

  // planning
  int64_t nr_plan_reclaimed = 0;
  auto nr_idles = idles.size();
  auto nr_candidates = candidates.size();
  for (int i = 0; i < nr_idles; i++) {
    auto idle = idles[i];
    int64_t max_reclaim = idle->lat_critical_ ? kMaxReclaim / 8 : kMaxReclaim;
    int64_t nr_to_reclaim =
        std::min<int64_t>(max_reclaim, idle->region_limit_ - 1);
    plan_idle_reclaims[i] = -nr_to_reclaim;
    nr_plan_reclaimed += nr_to_reclaim;
  }

  int64_t nr_plan_granted = 0;
  int gainer_idx = 0;
  int victim_idx = candidates.size();
  while (gainer_idx < victim_idx) {
    if (nr_plan_granted < nr_plan_reclaimed) {
      auto candidate = candidates[gainer_idx];
      int64_t nr_to_grant =
          candidate->almost_full()
              ? kMaxGrant * std::log2(candidate->stats.perf_gain) / max_gain
              : 0;
      plan_adjusts[gainer_idx] = nr_to_grant;
      nr_plan_granted += nr_to_grant;
      gainer_idx++;
    } else {
      victim_idx--;
      auto victim = candidates[victim_idx];
      int64_t max_reclaim =
          victim->lat_critical_ ? kMaxReclaim / 8 : kMaxReclaim;
      int64_t nr_to_reclaim = std::min<int64_t>(
          max_reclaim * std::log2(victim->stats.perf_gain) / min_gain,
          victim->region_limit_ - 1);
      plan_adjusts[victim_idx] = -nr_to_reclaim;
      nr_plan_reclaimed += nr_to_reclaim;
    }
  }
  assert(gainer_idx == victim_idx);
  if (gainer_idx == candidates.size()) {
    if (nr_plan_granted < nr_plan_reclaimed) {
      // All active clients get their portion but still too much reclaimed
      // memory. So we return them back to idle clients evenly.
      int64_t nr_overchaged = nr_plan_reclaimed - nr_plan_granted;
      int64_t nr_remain = nr_overchaged;
      for (int i = 0; i < nr_idles; i++) {
        if (plan_idle_reclaims[i] == 0)
          continue;
        int64_t nr_to_return = std::min(
            nr_remain,
            (nr_overchaged * (-plan_idle_reclaims[i]) + nr_plan_reclaimed) /
                nr_plan_reclaimed); // proportionally return over charged memory
        plan_idle_reclaims[i] += nr_to_return;
        nr_remain -= nr_to_return;
        if (nr_remain <= 0)
          break;
      }
    } else {
      plan_adjusts[gainer_idx - 1] += nr_plan_reclaimed - nr_plan_granted;
    }
  } else { // adjust the last client's grant
           // granted more than reclaimed. Try to reclaim more.
    if (!candidates[gainer_idx]->lat_critical_) {
      plan_adjusts[gainer_idx] += nr_plan_reclaimed - nr_plan_granted;
    } else { // protect the lat-critical app
      int64_t nr_overgranted = nr_plan_granted - nr_plan_reclaimed;
      int64_t nr_remain = nr_overgranted;
      int64_t nr_per_client = (nr_remain + gainer_idx - 1) / gainer_idx;
      for (int i = gainer_idx - 1; i >= 0; i--) {
        plan_adjusts[i] -= std::min(nr_remain, nr_per_client);
        nr_remain -= nr_per_client;
        if (nr_remain <= 0)
          break;
      }
    }
  }

  // applying
  bool rebalanced = false;
  for (int i = 0; i < nr_idles; i++) {
    auto idle = idles[i];
    MIDAS_LOG(kError) << idle->stats.perf_gain << " " << plan_idle_reclaims[i];
    if (plan_idle_reclaims[i] == 0)
      continue;
    idle->update_limit(idle->region_limit_ + plan_idle_reclaims[i]);
    rebalanced = true;
  }
  for (int i = 0; i < nr_candidates; i++) {
    auto candidate = candidates[i];
    MIDAS_LOG(kError) << candidate->stats.perf_gain << " " << plan_adjusts[i];
    if (plan_adjusts[i] == 0)
      continue;
    candidate->update_limit(candidate->region_limit_ + plan_adjusts[i]);
    rebalanced = true;
  }

  if (rebalanced) {
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

void Daemon::on_mem_rebalance_cliffhanger() {
  if (!kEnableDynamicRebalance)
    return;
  static uint64_t kStepSize = 16ul;

  std::vector<std::shared_ptr<Client>> clients;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    for (auto &[_, client] : clients_) {
      if (client->region_limit_ <= 1)
        continue;
      clients.emplace_back(client);
    }
  }
  if (clients.size() <= 1)
    return;

  std::sort(clients.begin(), clients.end(),
            [](std::shared_ptr<Client> c1, std::shared_ptr<Client> c2) {
              const auto &s1 = c1->stats;
              const auto &s2 = c2->stats;
              float h1 = -1., h2 = -1.;
              if (s1.hits + s1.misses > 0)
                h1 = static_cast<float>(s1.hits + s1.vhits) /
                         (s1.hits + s1.vhits + s1.misses) -
                     static_cast<float>(s1.hits) / (s1.hits + s1.misses);
              if (s2.hits + s2.misses)
                h2 = static_cast<float>(s2.hits + s2.vhits) /
                         (s2.hits + s2.vhits + s2.misses) -
                     static_cast<float>(s2.hits) / (s2.hits + s2.misses);

              return h1 < h2;
            });

  bool rebalanced = false;
  int victim_idx = 0;
  int winner_idx = clients.size() - 1;
  while (victim_idx < winner_idx) {
    // find a winner candidate
    while (winner_idx > victim_idx) {
      auto winner = clients[winner_idx];
      if (winner->almost_full())
        break;
      winner_idx--;
    }
    if (winner_idx == victim_idx)
      break;

    auto winner = clients[winner_idx];
    auto victim = clients[victim_idx];

    auto nr_to_reclaim = std::min(victim->region_limit_ - 1, kStepSize);
    victim->update_limit(victim->region_limit_ - nr_to_reclaim);
    winner->update_limit(winner->region_limit_ + nr_to_reclaim);
    if (nr_to_reclaim)
      rebalanced = true;

    victim_idx++;
  }
  if (rebalanced) {
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
    case SET_WEIGHT:
      do_set_weight(msg);
      break;
    case SET_LAT_CRITICAL:
      do_set_lat_critical(msg);
      break;
    default:
      MIDAS_LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }

  MIDAS_LOG(kInfo) << "Daemon stopped to serve...";
}

namespace utils {
uint64_t check_sys_avail_mem() {
  struct sysinfo info;
  if (sysinfo(&info) != 0) {
    MIDAS_LOG(kError) << "Error when calling sysinfo()";
    return -1;
  }

  uint64_t avail_mem_bytes = info.freeram * info.mem_unit;
  MIDAS_LOG(kDebug) << "Available Memory: " << avail_mem_bytes / (1024 * 1024)
                    << " MB";
  return avail_mem_bytes;
}

uint64_t check_file_avail_mem() {
  std::ifstream mem_cfg(kMemoryCfgFile);
  if (!mem_cfg.is_open()) {
    MIDAS_LOG(kError) << "open " << kMemoryCfgFile << " failed!";
    return -1;
  }
  uint64_t upd_mem_limit;
  mem_cfg >> upd_mem_limit;
  mem_cfg.close();
  return upd_mem_limit;
}

Daemon::Policy check_policy() {
  std::ifstream policy_cfg(kPolicyCfgFile);
  int new_policy;
  policy_cfg >> new_policy;
  policy_cfg.close();
  if (new_policy < Daemon::Policy::Static ||
      new_policy >= Daemon::Policy::NumPolicy)
    return Daemon::Policy::Invalid;
  return Daemon::Policy(new_policy);
}
} // namespace utils

} // namespace midas
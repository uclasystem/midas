#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "base_soft_mem_pool.hpp"
#include "cache_manager.hpp"
#include "evacuator.hpp"
#include "logging.hpp"
#include "qpair.hpp"
#include "resource_manager.hpp"
#include "shm_types.hpp"
#include "time.hpp"
#include "utils.hpp"

namespace midas {
constexpr static int32_t kMaxAllocRetry = 5;
constexpr static int32_t kReclaimRepeat = 20;
constexpr static auto kReclaimTimeout = std::chrono::milliseconds(100);  // ms
constexpr static auto kAllocRetryDelay = std::chrono::microseconds(100); // us
constexpr static int32_t kMonitorTimeout = 1; // seconds
constexpr static int32_t kDisconnTimeout = 3; // seconds
constexpr static bool kEnableFreeList = true;
constexpr static int32_t kFreeListSize = 512;

std::atomic_int64_t Region::global_mapped_rid_{0};

Region::Region(uint64_t pid, uint64_t region_id) noexcept
    : pid_(pid), prid_(region_id), vrid_(INVALID_VRID) {
  map();
}

void Region::map() noexcept {
  assert(vrid_ == INVALID_VRID);
  const auto rwmode = boost::interprocess::read_write;
  const std::string shm_name_ = utils::get_region_name(pid_, prid_);
  SharedMemObj shm_obj(boost::interprocess::open_only, shm_name_.c_str(),
                       rwmode);
  shm_obj.get_size(size_);
  vrid_ = global_mapped_rid_.fetch_add(1);
  void *addr = reinterpret_cast<void *>(kVolatileSttAddr + vrid_ * kRegionSize);
  shm_region_ = std::make_unique<MappedRegion>(shm_obj, rwmode, 0, size_, addr);
}

void Region::unmap() noexcept {
  shm_region_.reset();
  vrid_ = INVALID_VRID;
}

Region::~Region() noexcept {
  unmap();
  free();
}

void Region::free() noexcept {
  SharedMemObj::remove(utils::get_region_name(pid_, prid_).c_str());
}

ResourceManager::ResourceManager(BaseSoftMemPool *cpool,
                                 const std::string &daemon_name) noexcept
    : cpool_(cpool), id_(get_unique_id()), region_limit_(0),
      txqp_(std::make_shared<QSingle>(utils::get_sq_name(daemon_name, false),
                                      false),
            std::make_shared<QSingle>(utils::get_ackq_name(daemon_name, id_),
                                      true)),
      rxqp_(std::to_string(id_), true), stop_(false), nr_pending_(0), stats_() {
  handler_thd_ = std::make_shared<std::thread>([&]() { pressure_handler(); });
  if (!cpool_)
    cpool_ = CachePool::global_cache_pool();
  assert(cpool_);
  connect(daemon_name);
}

ResourceManager::~ResourceManager() noexcept {
  stop_ = true;
  handler_thd_->join();

  disconnect();
  rxqp_.destroy();
  txqp_.RecvQ().destroy();
}

std::shared_ptr<ResourceManager>
ResourceManager::global_manager_shared_ptr() noexcept {
  return CachePool::global_cache_pool()->rmanager_;
}

/** trigger evacuation */
bool ResourceManager::reclaim_trigger() noexcept {
  return reclaim_target() > 0;
}

int64_t ResourceManager::reclaim_target() noexcept {
  constexpr static float kAvailRatioThresh = 0.01;
  int64_t nr_avail = NumRegionAvail();
  int64_t nr_limit = NumRegionLimit();
  if (nr_limit <= 1)
    return 0;
  int64_t target_avail = nr_limit * kAvailRatioThresh;
  int64_t nr_to_reclaim = nr_pending_;
  auto headroom = std::max<int64_t>(reclaim_headroom(), target_avail);
  if (nr_avail <= headroom)
    nr_to_reclaim += std::max(headroom - nr_avail, 2l);
  nr_to_reclaim = std::min(nr_to_reclaim, nr_limit);
  return nr_to_reclaim;
}

int32_t ResourceManager::reclaim_headroom() noexcept {
  auto scale_factor = 5;
  if (stats_.alloc_tput > 1000 || stats_.alloc_tput > 8 * stats_.reclaim_tput)
    scale_factor = 20;
  else if (stats_.alloc_tput > 500 ||
           stats_.alloc_tput > 6 * stats_.reclaim_tput)
    scale_factor = 15;
  else if (stats_.alloc_tput > 300)
    scale_factor = 10;
  auto headroom = std::min<int32_t>(
      region_limit_ * 0.5,
      std::max<int32_t>(32,
                        scale_factor * stats_.reclaim_dur * stats_.alloc_tput));
  stats_.headroom = headroom;
  // MIDAS_LOG(kInfo) << "headroom: " << headroom;
  return headroom;
}

int32_t ResourceManager::reclaim_nr_thds() noexcept {
  int32_t nr_evac_thds = kNumEvacThds;
  if (stats_.reclaim_tput > 1e-6) { // having non-zero reclaim_tput
    nr_evac_thds = stats_.alloc_tput / stats_.reclaim_tput;
    if (nr_evac_thds < 4)
      nr_evac_thds++;
    else
      nr_evac_thds *= 2;
    nr_evac_thds = std::min<int32_t>(kNumEvacThds, nr_evac_thds);
    nr_evac_thds = std::max<int32_t>(1, nr_evac_thds);
  }
  return nr_evac_thds;
}

/** Profiling for stats */
inline void ResourceManager::prof_alloc_tput() {
  auto time = Time::get_us();
  if (stats_.prev_time == 0) { // init
    stats_.prev_time = time;
    stats_.prev_alloced = stats_.nr_alloced;
  } else {
    auto dur_us = time - stats_.prev_time;
    auto alloc_tput = (stats_.nr_alloced - stats_.prev_alloced) * 1e6 / dur_us;
    stats_.alloc_tput = alloc_tput;
    stats_.prev_time = time;
    stats_.prev_alloced = stats_.nr_alloced;
    MIDAS_LOG(kDebug) << "Allocation Tput: " << alloc_tput;
  }

  if (stats_.accum_evac_dur > 1e-6 &&
      stats_.accum_nr_reclaimed >= 1) { // > 1us && reclaimed > 1 segment
    stats_.reclaim_tput = stats_.accum_nr_reclaimed / stats_.accum_evac_dur;
    stats_.reclaim_dur = stats_.accum_evac_dur / stats_.evac_cnt;
    MIDAS_LOG(kDebug) << "Reclamation Tput: " << stats_.reclaim_tput
                      << ", Duration: " << stats_.reclaim_dur;
    // reset accumulated counters
    stats_.evac_cnt = 0;
    stats_.accum_nr_reclaimed = 0;
    stats_.accum_evac_dur = 0;
  }
  MIDAS_LOG(kDebug) << "Evacuator params: headroom = " << reclaim_headroom()
                    << ", nr_thds = " << reclaim_nr_thds();
}

void ResourceManager::prof_reclaim_stt() {
  stats_.prev_evac_alloced = stats_.nr_evac_alloced;
  stats_.prev_freed = stats_.nr_freed;
}

void ResourceManager::prof_reclaim_end(int nr_thds, double dur_s) {
  auto nr_freed = stats_.nr_freed - stats_.prev_freed;
  auto nr_evac_alloced = stats_.nr_evac_alloced - stats_.prev_evac_alloced;
  stats_.accum_nr_reclaimed +=
      static_cast<float>(nr_freed - nr_evac_alloced) / nr_thds;
  stats_.accum_evac_dur += static_cast<float>(dur_s); // to us
  stats_.evac_cnt++;
}

/** interacting with the global daemon */
int ResourceManager::connect(const std::string &daemon_name) noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  try {
    unsigned int prio = 0;
    CtrlMsg msg{.id = id_, .op = CtrlOpCode::CONNECT};

    txqp_.send(&msg, sizeof(CtrlMsg));
    int ret = txqp_.recv(&msg, sizeof(CtrlMsg));
    if (ret) {
      return -1;
    }
    if (msg.op == CtrlOpCode::CONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      MIDAS_LOG(kInfo) << "Connection established.";
    else {
      MIDAS_LOG(kError) << "Connection failed.";
      abort();
    }
    region_limit_ = msg.mmsg.size / kRegionSize;
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }

  return 0;
}

int ResourceManager::disconnect() noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  try {
    unsigned int prio = 0;
    CtrlMsg msg{.id = id_, .op = CtrlOpCode::DISCONNECT};

    txqp_.send(&msg, sizeof(CtrlMsg));
    int ret = txqp_.timed_recv(&msg, sizeof(CtrlMsg), kDisconnTimeout);
    if (ret)
      return -1;
    if (msg.op == CtrlOpCode::DISCONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      MIDAS_LOG(kInfo) << "Connection destroyed.";
    else {
      MIDAS_LOG(kError) << "Disconnection failed.";
      return -1;
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }

  return 0;
}

void ResourceManager::pressure_handler() {
  MIDAS_LOG(kInfo) << "pressure handler thd is running...";

  while (!stop_) {
    CtrlMsg msg;
    if (rxqp_.timed_recv(&msg, sizeof(msg), kMonitorTimeout) != 0)
      continue;

    MIDAS_LOG(kDebug) << "PressureHandler recved msg " << msg.op;
    switch (msg.op) {
    case UPDLIMIT:
      do_update_limit(msg);
      break;
    case PROF_STATS:
      do_profile_stats(msg);
      break;
    case FORCE_RECLAIM:
      do_force_reclaim(msg);
      break;
    case DISCONNECT:
      do_disconnect(msg);
    default:
      MIDAS_LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }
}

void ResourceManager::do_update_limit(CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto new_region_limit = msg.mmsg.size;
  MIDAS_LOG(kInfo) << "Client " << id_ << " update limit: " << region_limit_
                   << "->" << new_region_limit;
  region_limit_ = new_region_limit;

  CtrlMsg ack{.op = CtrlOpCode::UPDLIMIT, .ret = CtrlRetCode::MEM_SUCC};
  if (NumRegionAvail() < 0) { // under memory pressure
    auto before_usage = NumRegionInUse();
    MIDAS_LOG_PRINTF(kInfo, "Memory shrinkage: %ld to reclaim (%ld->%ld).\n",
                     -NumRegionAvail(), NumRegionInUse(), NumRegionLimit());
    if (!reclaim()) // failed to reclaim enough memory
      ack.ret = CtrlRetCode::MEM_FAIL;
    auto after_usage = NumRegionInUse();
    auto nr_reclaimed = before_usage - after_usage;
    MIDAS_LOG_PRINTF(kInfo, "Memory shrinkage: %ld reclaimed (%ld/%ld).\n",
                     nr_reclaimed, NumRegionInUse(), NumRegionLimit());
  } else if (reclaim_trigger()) { // concurrent GC if needed
    cpool_->get_evacuator()->signal_gc();
  }
  ack.mmsg.size = region_map_.size() + freelist_.size();
  rxqp_.send(&ack, sizeof(ack));
}

void ResourceManager::do_force_reclaim(CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto new_region_limit = msg.mmsg.size;
  MIDAS_LOG(kInfo) << "Client " << id_ << " update limit: " << region_limit_
                   << "->" << new_region_limit;
  region_limit_ = new_region_limit;

  force_reclaim();

  CtrlMsg ack{.op = CtrlOpCode::UPDLIMIT, .ret = CtrlRetCode::MEM_SUCC};
  ack.mmsg.size = region_map_.size() + freelist_.size();
  rxqp_.send(&ack, sizeof(ack));
}

void ResourceManager::do_profile_stats(CtrlMsg &msg) {
  StatsMsg stats{0};
  cpool_->profile_stats(&stats);
  prof_alloc_tput();
  stats.headroom = stats_.headroom;
  rxqp_.send(&stats, sizeof(stats));
}

void ResourceManager::do_disconnect(CtrlMsg &msg) {
  stop_ = true;
  handler_thd_->join();
  MIDAS_LOG(kInfo) << "Client " << id_ << " Disconnected!";
  exit(-1);
}

bool ResourceManager::reclaim() {
  if (NumRegionInUse() > NumRegionLimit() + kForceReclaimThresh)
    return force_reclaim();
  if (NumRegionInUse() < NumRegionLimit())
    return true;

  nr_pending_++;
  cpool_->get_evacuator()->signal_gc();
  for (int rep = 0; rep < kReclaimRepeat; rep++) {
    {
      std::unique_lock<std::mutex> ul(mtx_);
      while (!freelist_.empty()) {
        auto region = freelist_.back();
        freelist_.pop_back();
        free_region(region, true);
      }
      cv_.wait_for(ul, kReclaimTimeout, [&] { return NumRegionAvail() > 0; });
    }
    if (NumRegionAvail() > 0)
      break;
    cpool_->get_evacuator()->signal_gc();
  }

  nr_pending_--;
  return NumRegionAvail() > 0;
}

bool ResourceManager::force_reclaim() {
  if (NumRegionInUse() < NumRegionLimit())
    return true;

  nr_pending_++;
  {
    std::unique_lock<std::mutex> ul(mtx_);
    while (!freelist_.empty()) {
      auto region = freelist_.back();
      freelist_.pop_back();
      free_region(region, true);
    }
  }

  while (NumRegionAvail() <= 0)
    cpool_->get_evacuator()->force_reclaim();
  nr_pending_--;
  return NumRegionAvail() > 0;
}

void ResourceManager::SetWeight(float weight) noexcept {
  CtrlMsg msg{
      .id = id_, .op = CtrlOpCode::SET_WEIGHT, .mmsg = {.weight = weight}};
  txqp_.send(&msg, sizeof(msg));
}

void ResourceManager::SetLatCritical(bool value) noexcept {
  CtrlMsg msg{.id = id_,
              .op = CtrlOpCode::SET_LAT_CRITICAL,
              .mmsg = {.lat_critical = value}};
  txqp_.send(&msg, sizeof(msg));
}

void ResourceManager::UpdateLimit(size_t size) noexcept {
  CtrlMsg msg{
      .id = id_, .op = CtrlOpCode::UPDLIMIT_REQ, .mmsg = {.size = size}};
  txqp_.send(&msg, sizeof(msg));
}

int64_t ResourceManager::AllocRegion(bool overcommit) noexcept {
  int retry_cnt = 0;
retry:
  if (retry_cnt >= kMaxAllocRetry) {
    MIDAS_LOG(kDebug) << "Cannot allocate new region after " << retry_cnt
                      << " retires!";
    return -1;
  }
  retry_cnt++;
  if (!overcommit && reclaim_trigger()) {
    if (NumRegionAvail() <= 0) { // block waiting for reclamation
      if (kEnableFaultHandler && retry_cnt >= kMaxAllocRetry / 2)
        force_reclaim();
      else
        reclaim();
    } else
      cpool_->get_evacuator()->signal_gc();
  }

  // 1) Fast path. Allocate from freelist
  std::unique_lock<std::mutex> lk(mtx_);
  if (!freelist_.empty()) {
    auto region = freelist_.back();
    freelist_.pop_back();
    region->map();
    int64_t region_id = region->ID();
    region_map_[region_id] = region;
    overcommit ? stats_.nr_evac_alloced++ : stats_.nr_alloced++;
    return region_id;
  }
  // 2) Local alloc path. Do reclamation and try local allocation again
  if (!overcommit && NumRegionAvail() <= 0) {
    lk.unlock();
    std::this_thread::sleep_for(kAllocRetryDelay);
    goto retry;
  }
  // 3) Remote alloc path. Comm with daemon and try to alloc
  CtrlMsg msg{.id = id_,
              .op = overcommit ? CtrlOpCode::OVERCOMMIT : CtrlOpCode::ALLOC,
              .mmsg = {.size = kRegionSize}};
  txqp_.send(&msg, sizeof(msg));

  unsigned prio;
  CtrlMsg ret_msg;
  int ret = txqp_.recv(&ret_msg, sizeof(ret_msg));
  if (ret) {
    MIDAS_LOG(kError) << "Allocation error: " << ret;
    return -1;
  }
  if (ret_msg.ret != CtrlRetCode::MEM_SUCC) {
    lk.unlock();
    goto retry;
  }

  int64_t region_id = ret_msg.mmsg.region_id;
  assert(region_map_.find(region_id) == region_map_.cend());

  auto region = std::make_shared<Region>(id_, region_id);
  region_map_[region_id] = region;
  assert(region->Size() == ret_msg.mmsg.size);
  assert((reinterpret_cast<uint64_t>(region->Addr()) & (~kRegionMask)) == 0);

  MIDAS_LOG(kDebug) << "Allocated region: " << region->Addr() << " ["
                    << region->Size() << "]";
  overcommit ? stats_.nr_evac_alloced++ : stats_.nr_alloced++;
  return region_id;
}

void ResourceManager::FreeRegion(int64_t rid) noexcept {
  stats_.nr_freed++;
  std::unique_lock<std::mutex> lk(mtx_);
  auto region_iter = region_map_.find(rid);
  if (region_iter == region_map_.cend()) {
    MIDAS_LOG(kError) << "Invalid region_id " << rid;
    return;
  }
  int64_t freed_bytes = free_region(region_iter->second, false);
  if (freed_bytes == -1) {
    MIDAS_LOG(kError) << "Failed to free region " << rid;
  }
}

void ResourceManager::FreeRegions(size_t size) noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  size_t total_freed = 0;
  int nr_freed_regions = 0;
  while (!region_map_.empty()) {
    auto region = region_map_.begin()->second;
    int64_t freed_bytes = free_region(region, false);
    if (freed_bytes == -1) {
      MIDAS_LOG(kError) << "Failed to free region " << region->ID();
      continue;
    }
    total_freed += freed_bytes;
    nr_freed_regions++;
    if (total_freed >= size)
      break;
  }
  MIDAS_LOG(kInfo) << "Freed " << nr_freed_regions << " regions ("
                   << total_freed << "bytes)";
}

/** This function is supposed to be called inside a locked section */
inline size_t ResourceManager::free_region(std::shared_ptr<Region> region,
                                           bool enforce) noexcept {
  int64_t rid = region->ID();
  uint64_t rsize = region->Size();
  /* Only stash freed region into the free list when:
   *  (1) not enforce free (happen in reclamation);
   *  (2) still have avail memory budget (or else we will need to return extra
          memory allocated by the evacuator);
   *  (3) freelist is not full.
   */
  if (kEnableFreeList && !enforce && NumRegionAvail() > 0 &&
      freelist_.size() < kFreeListSize) {
    region->unmap();
    freelist_.emplace_back(region);
  } else {
    CtrlMsg msg{.id = id_,
                .op = CtrlOpCode::FREE,
                .mmsg = {.region_id = rid, .size = rsize}};
    txqp_.send(&msg, sizeof(msg));

    CtrlMsg ack;
    unsigned prio;
    int ret = txqp_.recv(&ack, sizeof(ack));
    assert(ret == 0);
    if (ack.op != CtrlOpCode::FREE || ack.ret != CtrlRetCode::MEM_SUCC)
      return -1;
    MIDAS_LOG(kDebug) << "Free region " << rid << " @ " << region->Addr();
  }

  region_map_.erase(rid);
  if (NumRegionAvail() > 0)
    cv_.notify_all();
  MIDAS_LOG(kDebug) << "region_map size: " << region_map_.size();
  return rsize;
}

} // namespace midas

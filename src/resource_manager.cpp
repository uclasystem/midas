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

#include "cache_manager.hpp"
#include "evacuator.hpp"
#include "logging.hpp"
#include "qpair.hpp"
#include "resource_manager.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {
constexpr static int32_t kMaxAllocRetry = 5;
constexpr static int32_t kReclaimRepeat = 10;
constexpr static auto kReclaimTimeout =
    std::chrono::milliseconds(100);           // milliseconds
constexpr static int32_t kMonitorTimeout = 1; // seconds
constexpr static int32_t kDisconnTimeout = 3; // seconds
constexpr static bool kEnableFreeList = false;
constexpr static int32_t kFreeListSize = 512;

Region::Region(uint64_t pid, uint64_t region_id) noexcept
    : pid_(pid), region_id_(region_id) {
  const auto rwmode = boost::interprocess::read_write;
  const std::string shm_name_ = utils::get_region_name(pid_, region_id_);
  SharedMemObj shm_obj(boost::interprocess::open_only, shm_name_.c_str(),
                       rwmode);
  shm_obj.get_size(size_);
  void *addr =
      reinterpret_cast<void *>(kVolatileSttAddr + region_id_ * kRegionSize);
  shm_region_ = std::make_shared<MappedRegion>(shm_obj, rwmode, 0, size_, addr);
}

Region::~Region() noexcept {
  SharedMemObj::remove(utils::get_region_name(pid_, region_id_).c_str());
}

ResourceManager::ResourceManager(CachePool *cpool,
                                 const std::string &daemon_name) noexcept
    : cpool_(cpool), id_(get_unique_id()), region_limit_(0),
      txqp_(std::make_shared<QSingle>(utils::get_sq_name(daemon_name, false),
                                      false),
            std::make_shared<QSingle>(utils::get_ackq_name(daemon_name, id_),
                                      true)),
      rxqp_(std::to_string(id_), true), stop_(false), nr_pending_(0),
      alloc_tput_stats_() {
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
  MIDAS_LOG(kError) << "pressure handler thd is running...";

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
  MIDAS_LOG(kError) << "Client " << id_ << " update limit: " << region_limit_
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
    MIDAS_LOG_PRINTF(kError, "Memory shrinkage: %ld reclaimed (%ld/%ld).\n",
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
  MIDAS_LOG(kError) << "Client " << id_ << " update limit: " << region_limit_
                    << "->" << new_region_limit;
  region_limit_ = new_region_limit;

  CtrlMsg ack{.op = CtrlOpCode::UPDLIMIT, .ret = CtrlRetCode::MEM_SUCC};
  ack.mmsg.size = region_map_.size() + freelist_.size();
  rxqp_.send(&ack, sizeof(ack));
}

void ResourceManager::do_profile_stats(CtrlMsg &msg) {
  StatsMsg stats{0};
  cpool_->profile_stats(&stats);
  int32_t headroom = std::min<int32_t>(
      768, std::max<int32_t>(1, std::max(region_limit_ * 0.1,
                                         alloc_tput_stats_.alloc_tput * 0.2)));
  // MIDAS_LOG(kError) << "headroom: " << headroom;
  stats.headroom = headroom;
  rxqp_.send(&stats, sizeof(stats));

  auto time = Time::get_us();
  if (alloc_tput_stats_.prev_time == 0) { // init
    alloc_tput_stats_.prev_time = time;
    alloc_tput_stats_.prev_alloced = alloc_tput_stats_.nr_alloced;
  } else {
    auto dur = time - alloc_tput_stats_.prev_time;
    auto alloc_tput =
        (alloc_tput_stats_.nr_alloced - alloc_tput_stats_.prev_alloced) * 1e6 /
        dur;
    alloc_tput_stats_.alloc_tput = alloc_tput;
    alloc_tput_stats_.prev_time = time;
    alloc_tput_stats_.prev_alloced = alloc_tput_stats_.nr_alloced;
    MIDAS_LOG(kDebug) << "Allocation Tput: " << alloc_tput;
  }
}

void ResourceManager::do_disconnect(CtrlMsg &msg) {
  stop_ = true;
  handler_thd_->join();
  MIDAS_LOG(kError) << "Client " << id_ << " Disconnected!";
  exit(-1);
}

bool ResourceManager::reclaim() {
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

void ResourceManager::SetWeight(int32_t weight) noexcept {
  CtrlMsg msg{.id = id_,
              .op = CtrlOpCode::SET_WEIGHT,
              .mmsg = {.size = static_cast<uint64_t>(weight)}};
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
      if (kEnableFaultHandler && retry_cnt >= kMaxAllocRetry)
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
    int64_t region_id = region->ID();
    region_map_[region_id] = region;
    alloc_tput_stats_.nr_alloced++;
    return region_id;
  }
  // 2) Local alloc path. Do reclamation and try local allocation again
  if (!overcommit && NumRegionAvail() <= 0) {
    lk.unlock();
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
    MIDAS_LOG(kError) << ": in recv msg, ret: " << ret;
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
  alloc_tput_stats_.nr_alloced++;
  return region_id;
}

void ResourceManager::FreeRegion(int64_t rid) noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  auto region_iter = region_map_.find(rid);
  if (region_iter == region_map_.cend()) {
    MIDAS_LOG(kError) << "Invalid region_id " << rid;
    return;
  }
  int64_t freed_bytes = free_region(region_iter->second, false);
  if (freed_bytes == -1) {
    MIDAS_LOG(kError) << "Failed to free region " << rid;
    return;
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

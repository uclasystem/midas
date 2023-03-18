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
constexpr static int kDisconnTimeout = 3; // seconds

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
    : cpool_(cpool), region_limit_(0), id_(get_unique_id()),
      txqp_(std::make_shared<QSingle>(utils::get_sq_name(daemon_name, false),
                                      false),
            std::make_shared<QSingle>(utils::get_ackq_name(daemon_name, id_),
                                      true)),
      rxqp_(std::to_string(id_), true), stop_(false) {
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
    if (rxqp_.try_recv(&msg, sizeof(msg)) == -1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      continue;
    }

    MIDAS_LOG(kDebug) << "PressureHandler recved msg " << msg.op;
    switch (msg.op) {
    case UPDLIMIT:
      do_update_limit(msg);
      break;
    case PROF_STATS:
      do_profile_stats(msg);
      break;
    default:
      MIDAS_LOG(kError) << "Recved unknown message: " << msg.op;
    }
  }
}

void ResourceManager::do_update_limit(CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);

  auto new_region_limit = msg.mmsg.size;
  MIDAS_LOG(kError) << region_limit_ << " " << new_region_limit;

  if (new_region_limit >= region_limit_) {
    region_limit_ = new_region_limit;
    CtrlMsg ack{.op = CtrlOpCode::UPDLIMIT, .ret = CtrlRetCode::MEM_SUCC};
    rxqp_.send(&ack, sizeof(ack));
  } else {
    int64_t nr_to_reclaim = region_limit_ - new_region_limit;
    region_limit_ = new_region_limit;
    do_reclaim(nr_to_reclaim);
  }
}

void ResourceManager::do_profile_stats(CtrlMsg &msg) {
  StatsMsg stats{0};
  cpool_->profile_stats(&stats);
  rxqp_.send(&stats, sizeof(stats));
}

inline void ResourceManager::do_reclaim(int64_t nr_to_reclaim) {
  assert(nr_to_reclaim > 0);
  cpool_->get_evacuator()->signal_gc();
  MIDAS_LOG_PRINTF(kError, "Memory shrinkage: %ld to reclaim.\n",
                   nr_to_reclaim);
  while (NumRegionAvail() < 0)
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  auto nr_reclaimed = nr_to_reclaim;

  MemMsg mm;
  CtrlRetCode ret = CtrlRetCode::MEM_SUCC;
  mm.size = nr_reclaimed;
  CtrlMsg ack{.op = CtrlOpCode::UPDLIMIT, .ret = ret, .mmsg = mm};
  rxqp_.send(&ack, sizeof(ack));
}

void ResourceManager::UpdateLimit(size_t size) noexcept {
  CtrlMsg msg{
      .id = id_, .op = CtrlOpCode::UPDLIMIT_REQ, .mmsg = {.size = size}};
  txqp_.send(&msg, sizeof(msg));
}

int64_t ResourceManager::AllocRegion(bool overcommit) noexcept {
retry:
  if (!overcommit && reclaim_trigger())
    cpool_->get_evacuator()->signal_gc();

  std::unique_lock<std::mutex> lk(mtx_);
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
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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
  return region_id;
}

void ResourceManager::FreeRegion(int64_t rid) noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  int64_t freed_bytes = free_region(rid);
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
    auto region_iter = region_map_.begin();
    int64_t freed_bytes = free_region(region_iter->second->ID());
    if (freed_bytes == -1) {
      MIDAS_LOG(kError) << "Failed to free region "
                        << region_iter->second->ID();
      // continue;
      break;
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
inline size_t ResourceManager::free_region(int64_t region_id) noexcept {
  auto region_iter = region_map_.find(region_id);
  if (region_iter == region_map_.cend()) {
    MIDAS_LOG(kError) << "Invalid region_id " << region_id;
    return -1;
  }

  size_t size = region_iter->second->Size();
  try {
    CtrlMsg msg{.id = id_,
                .op = CtrlOpCode::FREE,
                .mmsg = {.region_id = region_id, .size = size}};
    txqp_.send(&msg, sizeof(msg));

    MIDAS_LOG(kDebug) << "Free region " << region_id << " @ "
                      << region_iter->second->Addr();

    CtrlMsg ack;
    unsigned prio;
    int ret = txqp_.recv(&ack, sizeof(ack));
    assert(ret == 0);
    if (ack.op != CtrlOpCode::FREE || ack.ret != CtrlRetCode::MEM_SUCC)
      return -1;
  } catch (boost::interprocess::interprocess_exception &e) {
    MIDAS_LOG(kError) << e.what();
  }

  region_map_.erase(region_id);
  MIDAS_LOG(kDebug) << "region_map size: " << region_map_.size();
  return size;
}

} // namespace midas

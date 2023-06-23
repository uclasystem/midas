#pragma once

#include <atomic>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <thread>

#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;
using MappedRegion = boost::interprocess::mapped_region;

class Region {
public:
  Region(uint64_t pid, uint64_t region_id) noexcept;
  ~Region() noexcept;

  void map() noexcept;
  void unmap() noexcept;
  void free() noexcept;

  inline bool mapped() const noexcept { return vrid_ == INVALID_VRID; };

  inline friend bool operator<(const Region &lhs, const Region &rhs) noexcept {
    return lhs.prid_ < rhs.prid_;
  }

  inline void *Addr() const noexcept {
    return reinterpret_cast<void *>(
        vrid_ == INVALID_VRID ? INVALID_VRID
                              : kVolatileSttAddr + vrid_ * kRegionSize);
  }
  inline uint64_t ID() const noexcept { return prid_; }
  inline int64_t Size() const noexcept { return size_; }

private:
  // generating unique name for the region shared memory file
  uint64_t pid_;
  uint64_t prid_; // physical memory region id
  uint64_t vrid_; // mapped virtual memory region id
  std::unique_ptr<MappedRegion> shm_region_;
  int64_t size_; // int64_t to adapt to boost::interprocess::offset_t

  static std::atomic_int64_t
      global_mapped_rid_; // never reuse virtual addresses

  constexpr static uint64_t INVALID_VRID = -1ul;
};

class CachePool;
class ResourceManager {
public:
  ResourceManager(CachePool *cpool = nullptr,
                  const std::string &daemon_name = kNameCtrlQ) noexcept;
  ~ResourceManager() noexcept;

  int64_t AllocRegion(bool overcommit = false) noexcept;
  void FreeRegion(int64_t rid) noexcept;
  void FreeRegions(size_t size = kRegionSize) noexcept;
  inline VRange GetRegion(int64_t region_id) noexcept;

  void UpdateLimit(size_t size) noexcept;
  void SetWeight(int32_t weight) noexcept;

  uint64_t NumRegionInUse() const noexcept;
  uint64_t NumRegionLimit() const noexcept;
  int64_t NumRegionAvail() const noexcept;

  /** trigger evacuation */
  bool reclaim_trigger() noexcept;
  int64_t reclaim_target() noexcept;

  /** profiling stats */
  void prof_alloc_tput();
  // called by Evacuator to calculate reclaim tput
  void prof_reclaim_stt();
  void prof_reclaim_end(int nr_thds, double dur_s);

  static std::shared_ptr<ResourceManager> global_manager_shared_ptr() noexcept;
  static ResourceManager *global_manager() noexcept;

private:
  int connect(const std::string &daemon_name = kNameCtrlQ) noexcept;
  int disconnect() noexcept;
  size_t free_region(std::shared_ptr<Region> region, bool enforce) noexcept;

  void pressure_handler();
  void do_update_limit(CtrlMsg &msg);
  void do_force_reclaim(CtrlMsg &msg);
  void do_profile_stats(CtrlMsg &msg);
  void do_disconnect(CtrlMsg &msg);

  bool reclaim();
  bool force_reclaim();

  CachePool *cpool_;

  // inter-process comm
  uint64_t id_;
  std::mutex mtx_;
  std::condition_variable cv_;
  QPair txqp_;
  QPair rxqp_;

  // regions
  uint64_t region_limit_;
  std::map<int64_t, std::shared_ptr<Region>> region_map_;
  std::list<std::shared_ptr<Region>> freelist_;

  std::atomic_int_fast64_t nr_pending_;
  std::shared_ptr<std::thread> handler_thd_;
  bool stop_;

  // stats
  struct AllocTputStats {
    // Updated by ResourceManager
    std::atomic_int_fast64_t nr_alloced{0};
    std::atomic_int_fast64_t nr_evac_alloced{
        0}; // temporarily allocated by the evacuator during evacuation
    std::atomic_int_fast64_t nr_freed{0};
    uint64_t prev_time{0};
    int64_t prev_alloced{0};
    float alloc_tput{0};
    float reclaim_tput{0}; // per-evacuator-thread
    int32_t headroom{0};
    // Updated by Evacuator
    int64_t prev_evac_alloced{0};
    int64_t prev_freed{0};
    float accum_evac_dur{0};
    float accum_nr_reclaimed{0};
  } stats_;
};

} // namespace midas

#include "impl/resource_manager.ipp"
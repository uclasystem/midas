#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "object.hpp"

namespace cachebank {

enum class EvacState { Succ, Fail, Fault, DelayRelease };

class LogSegment;   // defined in log.hpp
class SegmentList;  // defined in log.hpp
class LogAllocator; // defined in log.hpp

class CachePool; // defined in cache_manager.hpp

class ResourceManager; // defined in resource_manager.hpp

class Evacuator {
public:
  Evacuator(CachePool *pool, std::shared_ptr<LogAllocator> allocator);
  ~Evacuator();
  void signal_gc();
  int64_t serial_gc();
  void parallel_gc(int nr_workers);

private:
  void init();

  int64_t gc(SegmentList &stash_list);

  /** Segment opeartions */
  EvacState scan_segment(LogSegment *segment, bool deactivate);
  EvacState evac_segment(LogSegment *segment);
  EvacState free_segment(LogSegment *segment);

  /** Helper funcs */
  bool segment_ready(LogSegment *segment);
  bool iterate_segment(LogSegment *segment, uint64_t &pos, ObjectPtr &optr);

  CachePool *pool_;
  std::shared_ptr<LogAllocator> allocator_;
  std::shared_ptr<ResourceManager> rmanager_;

  bool terminated_;

  std::shared_ptr<std::thread> gc_thd_;
  std::condition_variable gc_cv_;
  std::mutex gc_mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
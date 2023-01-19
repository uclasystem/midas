#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace cachebank {

enum class EvacState { Succ, Fail, Fault, DelayRelease };

class LogChunk;
class LogSegment;
class ObjectPtr;
class SegmentList;

class Evacuator {
public:
  Evacuator();
  ~Evacuator();
  void signal_gc();
  int64_t serial_gc();
  void parallel_gc(int nr_workers);

  static Evacuator *global_evacuator();

private:
  void init();

  int64_t gc(SegmentList &stash_list);

  /** Segment opeartions */
  EvacState scan_segment(LogSegment *segment, bool deactivate);
  EvacState evac_segment(LogSegment *segment);
  EvacState free_segment(LogSegment *segment);

  /** Chunk opeartions */
  EvacState scan_chunk(LogChunk *chunk, bool deactivate);
  EvacState evac_chunk(LogChunk *chunk);
  EvacState free_chunk(LogChunk *chunk);

  /** Helper funcs */
  bool segment_ready(LogSegment *segment);
  bool iterate_chunk(LogChunk *chunk, uint64_t &pos, ObjectPtr &optr);

  bool terminated_;

  std::shared_ptr<std::thread> gc_thd_;
  std::condition_variable gc_cv_;
  std::mutex gc_mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
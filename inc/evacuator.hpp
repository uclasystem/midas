#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

namespace cachebank {

class LogChunk;
class LogSegment;
class ObjectPtr;

class Evacuator {
public:
  Evacuator();
  ~Evacuator();
  int64_t gc();
  void signal_gc();

  static Evacuator *global_evacuator();

private:
  constexpr static int kNumGCThds = 48;
  constexpr static int kNumScanThds = 8;

  void init();

  /** Segment opeartions */
  void scan_segment(LogSegment *segment, bool deactivate);
  bool evac_segment(LogSegment *segment);
  bool free_segment(LogSegment *segment);

  /** Chunk opeartions */
  int32_t scan_chunk(LogChunk *chunk, bool deactivate);
  bool evac_chunk(LogChunk *chunk);
  bool free_chunk(LogChunk *chunk);

  /** Helper funcs */
  bool segment_ready(LogSegment *segment);
  bool iterate_chunk(LogChunk *chunk, uint64_t &pos, ObjectPtr &optr);
  template <class C, class T>
  void parallelizer(int nr_workers, C &work, std::function<bool(T)> fn);

  int32_t nr_gc_thds_;
  std::mutex mtx_;
  bool terminated_;

  std::shared_ptr<std::thread> gc_thd_;
  std::condition_variable gc_cv_;
  std::mutex gc_mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
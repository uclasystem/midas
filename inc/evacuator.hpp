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
  void signal_gc();
  int64_t gc();

  static Evacuator *global_evacuator();

private:
  void init();

  /** Segment opeartions */
  bool scan_segment(LogSegment *segment, bool deactivate);
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

  bool terminated_;

  std::shared_ptr<std::thread> gc_thd_;
  std::condition_variable gc_cv_;
  std::mutex gc_mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
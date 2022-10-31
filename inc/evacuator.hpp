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
  void scan(int nr_thds = kNumScanThds);
  int64_t gc();
  void signal_scan();
  void signal_gc();

  static Evacuator *global_evacuator();

  /** [Deprecated] */
  int64_t stw_gc(int64_t nr_to_reclaim);
  int64_t conc_gc(int nr_thds = kNumGCThds);
  void evacuate(int nr_thds = kNumGCThds);

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

  int32_t under_pressure_;
  int32_t nr_gc_thds_;
  std::mutex mtx_;
  bool terminated_;

  std::shared_ptr<std::thread> scanner_thd_;
  std::condition_variable scanner_cv_;
  std::mutex scanner_mtx_;
  std::chrono::steady_clock::time_point scan_ts;

  std::shared_ptr<std::thread> evacuator_thd_;
  std::condition_variable evacuator_cv_;
  std::mutex evacuator_mtx_;

  constexpr static float kAliveIncStep = 0.1;
  constexpr static float kAliveThresholdLow = 0.6;
  constexpr static float kAliveThresholdHigh = 0.9;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
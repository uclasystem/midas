#pragma once

#include <atomic>
#include <functional>
#include <mutex>

namespace cachebank {

class LogChunk;
class LogSegment;
class ObjectPtr;

class Evacuator {
public:
  Evacuator();
  // ~Evacuator();
  int64_t stw_gc(int64_t nr_to_reclaim);
  int64_t conc_gc(int nr_thds = kNumGCThds);
  void evacuate(int nr_thds = kNumGCThds);
  void scan(int nr_thds = kNumGCThds);

  static Evacuator *global_evacuator();

private:
  constexpr static int kNumGCThds = 48;

  void init();

  void scan_segment(LogSegment *segment, bool deactivate);
  bool evac_segment(LogSegment *segment);
  bool free_segment(LogSegment *segment);

  bool iterate_chunk(LogChunk *chunk, uint64_t &pos, ObjectPtr &optr);
  bool scan_chunk(LogChunk *chunk, bool deactivate);
  bool evac_chunk(LogChunk *chunk);
  bool free_chunk(LogChunk *chunk);

  template <class C, class T>
  void parallelizer(int nr_workers, C &work, std::function<bool(T)> fn);

  int32_t under_pressure_;
  int32_t nr_gc_thds_;
  std::mutex mtx_;

  constexpr static float kAliveIncStep = 0.1;
  constexpr static float kAliveThresholdLow = 0.6;
  constexpr static float kAliveThresholdHigh = 0.9;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
#pragma once

#include <atomic>
#include <mutex>

namespace cachebank {

class LogChunk;
class LogRegion;

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

  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void free_region(LogRegion *region);
  bool evac_chunk(LogChunk *chunk);
  bool scan_chunk(LogChunk *chunk);
  bool free_chunk(LogChunk *chunk);

  using work_fn = void (Evacuator::*)(LogRegion *);
  void parallelizer(work_fn fn);

  int32_t under_pressure_;
  int32_t nr_gc_thds_;
  std::mutex mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
#pragma once

#include <atomic>
#include <mutex>

namespace cachebank {

class LogRegion;

class Evacuator {
public:
  Evacuator();
  // ~Evacuator();
  int64_t gc(int64_t nr_to_reclaim);
  void evacuate(int nr_thds = kNumGCThds);
  void scan(int nr_thds = kNumGCThds);

  static Evacuator *global_evacuator();

private:
  constexpr static int kNumGCThds = 40;

  void init();

  using work_fn = void (Evacuator::*)(LogRegion *);
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void parallelizer(work_fn fn);

  int32_t under_pressure_;

  int nr_gc_thds_;
  std::mutex mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
#pragma once

#include <atomic>

namespace cachebank {

class LogRegion;

class Evacuator {
public:
  Evacuator();
  // ~Evacuator();
  void evacuate(int nr_thds = kNumGCThds);
  void scan(int nr_thds = kNumGCThds);
  void gc(int nr_thds = kNumGCThds);

  static Evacuator *global_evacuator();

private:
  constexpr static int kNumGCThds = 1;

  void init();

  using work_fn = void (Evacuator::*)(LogRegion *);
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void parallelizer(work_fn fn);

  std::atomic_int_fast32_t nr_master_thd;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
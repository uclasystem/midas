#pragma once

namespace cachebank {

class LogRegion;

class Evacuator {
public:
  Evacuator();
  // ~Evacuator();
  void evacuate(int nr_thds = kNumGCThds);
  void scan(int nr_thds = kNumGCThds);

private:
  constexpr static int kNumGCThds = 1;

  void init();

  using work_fn = void (Evacuator::*)(LogRegion *);
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void parallelizer(work_fn fn);
};

} // namespace cachebank

#include "impl/evacuator.ipp"
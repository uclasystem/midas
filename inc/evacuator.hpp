#pragma once

namespace cachebank {

class LogRegion;

class Evacuator {
public:
  Evacuator(int nr_thds = 1);
  // ~Evacuator();
  void evacuate();
  void scan();

private:
  int nr_thds_;
  bool park_;
  void init();

  using work_fn = void (Evacuator::*)(LogRegion *);
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void parallelizer(work_fn fn);
};

} // namespace cachebank

#include "impl/evacuator.ipp"
#pragma once

#include "log.hpp"

namespace cachebank {

template<int nr_thds>
class Evacuator {
public:
  void evacuate();
  void scan();

private:
  using work_fn = void (Evacuator<nr_thds>::*)(LogRegion *);
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
  void parallelizer(work_fn fn);
};

} // namespace cachebank

#include "impl/evacuator.ipp"
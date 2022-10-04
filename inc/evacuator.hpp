#pragma once

#include "log.hpp"

namespace cachebank {

class Evacuator {
public:
  void evacuate();
  void scan();

private:
  void evac_region(LogRegion *region);
  void scan_region(LogRegion *region);
};

} // namespace cachebank
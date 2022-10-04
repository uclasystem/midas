#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "resource_manager.hpp"
#include <memory>

namespace cachebank {

void Evacuator::evacuate() {
  auto allocator = LogAllocator::global_allocator();
  auto nr_regions = allocator->vRegions_.size();
  for (auto region : allocator->vRegions_) {
    auto raw_ptr = region.get();
    if (raw_ptr)
      evac_region(raw_ptr);
  }
  for (int i = 0; i < allocator->vRegions_.size(); i++) {
    if (allocator->vRegions_[i]->destroyed()) {
      allocator->vRegions_.erase(allocator->vRegions_.begin() + i);
      i--;
    }
  }
  LOG(kError) << "Before evacuation: " << nr_regions
              << " regions";
  LOG(kError) << "After  evacuation: " << allocator->vRegions_.size()
              << " regions";
}

void Evacuator::scan() {
  auto allocator = LogAllocator::global_allocator();
  for (auto region : allocator->vRegions_) {
    auto raw_ptr = region.get();
    if (raw_ptr)
      scan_region(raw_ptr);
  }
}

inline void Evacuator::scan_region(LogRegion *region) {
  LOG(kError) << region->full();
  region->scan();
}

inline void Evacuator::evac_region(LogRegion *region) {
  LOG(kError) << region->full();
  region->evacuate();
}

} // namespace cachebank
#include <iterator>
#include <memory>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"

namespace cachebank {
void Evacuator::evacuate(int nr_thds) {
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  auto *tasks = new std::vector<LogRegion *>[nr_thds];

  int tid = 0;
  auto nr_regions = regions.size();
  for (auto region : regions) {
    auto raw_ptr = region.get();
    if (raw_ptr) {
      tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_thds;
    }
  }

  for (tid = 0; tid < nr_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : tasks[tid]) {
        evac_region(region);
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  allocator->cleanup_regions();

  LOG(kError) << "Before evacuation: " << nr_regions << " regions";
  LOG(kError) << "After  evacuation: " << regions.size() << " regions";

  delete[] tasks;
}

void Evacuator::scan(int nr_thds) {
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  auto *tasks = new std::vector<LogRegion *>[nr_thds];

  int tid = 0;
  auto nr_regions = regions.size();
  for (auto region : regions) {
    auto raw_ptr = region.get();
    if (raw_ptr) {
      tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_thds;
    }
  }

  for (tid = 0; tid < nr_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : tasks[tid]) {
        scan_region(region);
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  delete[] tasks;
}

inline void Evacuator::scan_region(LogRegion *region) {
  // LOG(kError) << region->full();
  region->scan();
}

inline void Evacuator::evac_region(LogRegion *region) {
  // LOG(kError) << region->full();
  region->evacuate();
}

} // namespace cachebank
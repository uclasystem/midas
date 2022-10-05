#pragma once

#include <iterator>
#include <memory>
#include <thread>
#include <vector>

#include "log.hpp"
#include "logging.hpp"

namespace cachebank {

template <int nr_thds> void Evacuator<nr_thds>::evacuate() {
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  std::vector<LogRegion *> tasks[nr_thds];

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
}

template <int nr_thds> void Evacuator<nr_thds>::scan() {
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  std::vector<LogRegion *> tasks[nr_thds];

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
}

template <int nr_thds>
inline void Evacuator<nr_thds>::scan_region(LogRegion *region) {
  // LOG(kError) << region->full();
  region->scan();
}

template <int nr_thds>
inline void Evacuator<nr_thds>::evac_region(LogRegion *region) {
  // LOG(kError) << region->full();
  region->evacuate();
}

} // namespace cachebank
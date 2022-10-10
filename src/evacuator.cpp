#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"

namespace cachebank {

void Evacuator::gc(int nr_thds) {
  if (nr_master_thd.fetch_add(1) > 0) {
    nr_master_thd.fetch_add(-1);
    return;
  }

  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  auto *scan_tasks = new std::vector<LogRegion *>[nr_thds];
  std::mutex evac_mtx;
  std::vector<LogRegion *> agg_evac_tasks;
  auto *evac_tasks = new std::vector<LogRegion *>[nr_thds];

  int tid = 0;
  auto nr_regions = regions.size();
  for (auto region : regions) {
    auto raw_ptr = region.get();
    if (raw_ptr) {
      scan_tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_thds;
    }
  }

  for (tid = 0; tid < nr_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : scan_tasks[tid]) {
        region->scan();
        auto alive_ratio = region->get_alive_ratio();
        if (alive_ratio < 0.5) {
          std::unique_lock<std::mutex> ul(evac_mtx);
          agg_evac_tasks.push_back(region);
        }
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] scan_tasks;

  int i = 0;
  for (auto region : agg_evac_tasks) {
    evac_tasks[i].push_back(region);
    i = (i+1) % nr_thds;
  }
  agg_evac_tasks.clear();

  for (tid = 0; tid < nr_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : evac_tasks[tid]) {
        evac_region(region);
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete [] evac_tasks;

  allocator->cleanup_regions();

  LOG(kError) << "Before evacuation: " << nr_regions << " regions";
  LOG(kError) << "After  evacuation: " << regions.size() << " regions";

  nr_master_thd.fetch_add(-1);
}

void Evacuator::evacuate(int nr_thds) {
  if (nr_master_thd.fetch_add(1) > 0) {
    nr_master_thd.fetch_add(-1);
    return;
  }

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
  nr_master_thd.fetch_add(-1);
}

void Evacuator::scan(int nr_thds) {
  if (nr_master_thd.fetch_add(1) > 0) {
    nr_master_thd.fetch_add(-1);
    return;
  }

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
  nr_master_thd.fetch_add(-1);
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
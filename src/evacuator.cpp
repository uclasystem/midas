#include <algorithm>
#include <chrono>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"

namespace cachebank {

int64_t Evacuator::stw_gc(int64_t nr_to_reclaim) {
  if (__sync_fetch_and_add(&under_pressure_, 1) > 0) {
    std::unique_lock<std::mutex> ul(mtx_);
    __sync_fetch_and_add(&under_pressure_, -1);
    return 1;
  }

  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;
  auto nr_scan_thds = nr_gc_thds_;
  auto nr_evac_thds = 1;

  std::vector<std::thread> gc_thds;
  auto *scan_tasks = new std::vector<LogRegion *>[nr_scan_thds];
  std::mutex evac_mtx;
  std::vector<std::pair<float, LogRegion *>> agg_evac_tasks;
  auto *evac_tasks = new std::vector<LogRegion *>[nr_evac_thds];

  int tid = 0;
  auto prev_nr_regions = regions.size();
  for (auto region : regions) {
    auto raw_ptr = region.get();
    if (raw_ptr) {
      scan_tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_scan_thds;
    }
  }

  for (tid = 0; tid < nr_scan_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : scan_tasks[tid]) {
        scan_region(region);
        auto alive_ratio = region->get_alive_ratio();
        if (alive_ratio > 0.9)
          continue;
        std::unique_lock<std::mutex> ul(evac_mtx);
        agg_evac_tasks.push_back(std::make_pair(alive_ratio, region));
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] scan_tasks;

  std::sort(
      agg_evac_tasks.begin(), agg_evac_tasks.end(),
      [](std::pair<float, LogRegion *> v1, std::pair<float, LogRegion *> v2) {
        return v1.first < v2.first;
      });

  tid = 0;
  for (auto [ar, region] : agg_evac_tasks) {
    evac_tasks[tid % nr_evac_thds].push_back(region);
    tid++;
  }
  agg_evac_tasks.clear();

  for (tid = 0; tid < nr_evac_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : evac_tasks[tid]) {
        evac_region(region);
        if (prev_nr_regions - regions.size() >= nr_to_reclaim)
          break;
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] evac_tasks;

  allocator->cleanup_regions();
  auto curr_nr_regions = regions.size();

  if (curr_nr_regions > prev_nr_regions - nr_to_reclaim) {
    int nr_remain = nr_to_reclaim - (prev_nr_regions - curr_nr_regions);
    int i = 0;
    for (auto &region : regions) {
      region->free();
      i++;
      if (i >= nr_remain)
        break;
    }
    allocator->cleanup_regions();
    curr_nr_regions = regions.size();
  }

  auto end = std::chrono::steady_clock::now();

  LOG(kDebug) << "STW GC: " << prev_nr_regions << " --> " << curr_nr_regions
              << " regions ("
              << std::chrono::duration<double>(end - stt).count() << "s).";

  __sync_fetch_and_add(&under_pressure_, -1);
  return prev_nr_regions - curr_nr_regions;
}

int64_t Evacuator::conc_gc(int nr_thds) {
  constexpr static float kAliveThresholdLow = 0.6;
  constexpr static float kAliveThresholdHigh = 0.9;
  static float kAliveThreshold = kAliveThresholdLow;

  if (under_pressure_ > 0)
    return 0;

  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;
  auto nr_scan_thds = nr_thds;
  auto nr_evac_thds = 1;

  std::vector<std::thread> gc_thds;
  auto *scan_tasks = new std::vector<LogRegion *>[nr_scan_thds];
  std::mutex evac_mtx;
  std::vector<std::pair<float, LogRegion *>> agg_evac_tasks;
  auto *evac_tasks = new std::vector<LogRegion *>[nr_evac_thds];

  int nr_reclaimed_regions = 0;
  int curr_nr_regions = 0;
  int prev_nr_regions = regions.size();
  int tid = 0;
  for (auto region : regions) {
    auto raw_ptr = region.get();
    if (raw_ptr) {
      scan_tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_scan_thds;
    }
  }

  for (tid = 0; tid < nr_scan_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : scan_tasks[tid]) {
        scan_region(region);
        auto alive_ratio = region->get_alive_ratio();
        if (alive_ratio <= kAliveThreshold) {
          std::unique_lock<std::mutex> ul(evac_mtx);
          agg_evac_tasks.push_back(std::make_pair(alive_ratio, region));
        }
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] scan_tasks;

  if (agg_evac_tasks.empty()) {
    kAliveThreshold =
        std::min<float>(kAliveThreshold + 0.1, kAliveThresholdHigh);
    goto done;
  } else {
    kAliveThreshold = kAliveThresholdLow;
  }

  std::sort(
      agg_evac_tasks.begin(), agg_evac_tasks.end(),
      [](std::pair<float, LogRegion *> v1, std::pair<float, LogRegion *> v2) {
        return v1.first < v2.first;
      });

  tid = 0;
  for (auto [ar, region] : agg_evac_tasks) {
    evac_tasks[tid % nr_evac_thds].push_back(region);
    tid++;
  }
  agg_evac_tasks.clear();

  for (tid = 0; tid < nr_evac_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : evac_tasks[tid]) {
        evac_region(region);
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] evac_tasks;

  nr_reclaimed_regions = allocator->cleanup_regions();

done:
  auto end = std::chrono::steady_clock::now();
  curr_nr_regions = regions.size();
  LOG(kDebug) << "Conc GC: reclaimed " << prev_nr_regions - curr_nr_regions
              << " regions ("
              << std::chrono::duration<double>(end - stt).count() << "s).";

  return prev_nr_regions - curr_nr_regions;
}

void Evacuator::evacuate(int nr_thds) {
  if (under_pressure_ > 0)
    return;
  std::unique_lock<std::mutex> ul(mtx_);

  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  auto *tasks = new std::vector<LogRegion *>[nr_thds];

  int tid = 0;
  auto prev_nr_regions = regions.size();
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
        if (under_pressure_ > 0)
          break;
        evac_region(region);
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  allocator->cleanup_regions();
  auto curr_nr_regions = regions.size();

  LOG(kError) << "Before evacuation: " << prev_nr_regions << " regions";
  LOG(kError) << "After  evacuation: " << curr_nr_regions << " regions";

  delete[] tasks;
}

void Evacuator::scan(int nr_thds) {
  if (under_pressure_ > 0)
    return;
  std::unique_lock<std::mutex> ul(mtx_);

  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  std::vector<std::thread> gc_thds;
  auto *tasks = new std::vector<LogRegion *>[nr_thds];

  int tid = 0;
  auto prev_nr_regions = regions.size();
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
        if (under_pressure_ > 0)
          break;
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
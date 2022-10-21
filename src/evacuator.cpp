#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"

namespace cachebank {

int64_t Evacuator::stw_gc(int64_t nr_to_reclaim) {
  if (__sync_fetch_and_add(&under_pressure_, 1) > 0) {
    std::unique_lock<std::mutex> ul(mtx_);
    __sync_fetch_and_add(&under_pressure_, -1);
    return 1;
  }
  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  std::atomic_int64_t nr_evaced = 0;
  auto rmanager = ResourceManager::global_manager();
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;
  auto nr_scan_thds = nr_gc_thds_;
  auto nr_evac_thds = 1;

  auto prev_nr_regions = regions.size();

  using Pair = std::pair<float, LogRegion *>;
  std::mutex evac_mtx;
  std::vector<Pair> agg_evac_tasks;

  parallelizer<decltype(regions), std::shared_ptr<LogRegion>>(
      nr_scan_thds, regions, [&](std::shared_ptr<LogRegion> region) {
        auto region_ = region.get();
        scan_region(region_, false);
        auto alive_ratio = region_->get_alive_ratio();
        if (alive_ratio > 0.9)
          return true;
        std::unique_lock<std::mutex> ul(evac_mtx);
        agg_evac_tasks.push_back(std::make_pair(alive_ratio, region_));
        return true;
      });

  if (!agg_evac_tasks.empty()) {
    std::sort(agg_evac_tasks.begin(), agg_evac_tasks.end(),
              [](Pair v1, Pair v2) { return v1.first < v2.first; });
    parallelizer<decltype(agg_evac_tasks), Pair>(
        nr_evac_thds, agg_evac_tasks, [&](Pair p) {
          if (evac_region(p.second))
            nr_evaced++;
          return rmanager->NumRegionInUse() + nr_to_reclaim >=
                 rmanager->NumRegionLimit();
        });
    agg_evac_tasks.clear();
    allocator->cleanup_regions();
  }

  if (rmanager->NumRegionInUse() + nr_to_reclaim >=
      rmanager->NumRegionLimit()) {
    for (auto &region : regions) {
      if (free_region(region.get()))
        nr_evaced++;
      auto rmanager = ResourceManager::global_manager();
      if (rmanager->NumRegionInUse() + nr_to_reclaim <
          rmanager->NumRegionLimit())
        break;
    }
    allocator->cleanup_regions();
  }

  auto end = std::chrono::steady_clock::now();
  __sync_fetch_and_add(&under_pressure_, -1);

  auto nr_reclaimed = rmanager->NumRegionLimit() - rmanager->NumRegionInUse();
  LOG(kInfo) << "STW GC: " << nr_evaced << " evacuated, " << nr_reclaimed
             << " reclaimed ("
             << std::chrono::duration<double>(end - stt).count() << "s).";
  return nr_reclaimed;
}

int64_t Evacuator::conc_gc(int nr_thds) {
  constexpr static float kAliveThresholdLow = 0.6;
  constexpr static float kAliveThresholdHigh = 0.9;
  static float kAliveThreshold = kAliveThresholdLow;

  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  std::atomic_int64_t nr_evaced = 0;
  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;
  auto nr_scan_thds = nr_thds;
  auto nr_evac_thds = 1;

  std::vector<std::thread> gc_thds;
  // auto *scan_tasks = new std::vector<LogRegion *>[nr_scan_thds];
  using Pair = std::pair<float, LogRegion *>;
  std::mutex evac_mtx;
  std::vector<Pair> agg_evac_tasks;
  auto *evac_tasks = new std::vector<LogRegion *>[nr_evac_thds];

  int tid = 0;
  parallelizer<decltype(regions), std::shared_ptr<LogRegion>>(
      nr_scan_thds, regions, [&](std::shared_ptr<LogRegion> region) {
        auto region_ = region.get();
        scan_region(region_, true);
        auto alive_ratio = region->get_alive_ratio();
        if (alive_ratio <= kAliveThreshold) {
          std::unique_lock<std::mutex> ul(evac_mtx);
          agg_evac_tasks.push_back(std::make_pair(alive_ratio, region_));
        }
        return true;
      });

  if (agg_evac_tasks.empty()) {
    kAliveThreshold =
        std::min<float>(kAliveThreshold + 0.1, kAliveThresholdHigh);
    goto done;
  } else {
    kAliveThreshold = kAliveThresholdLow;
  }
  std::sort(agg_evac_tasks.begin(), agg_evac_tasks.end(),
            [](Pair v1, Pair v2) { return v1.first < v2.first; });

  parallelizer<decltype(agg_evac_tasks), Pair>(nr_evac_thds, agg_evac_tasks,
                                               [&](Pair p) {
                                                 if (evac_region(p.second))
                                                   nr_evaced++;
                                                 return true;
                                               });
  agg_evac_tasks.clear();
  allocator->cleanup_regions();

done:
  auto end = std::chrono::steady_clock::now();
  auto rmanager = ResourceManager::global_manager();
  auto nr_reclaimed = rmanager->NumRegionLimit() - rmanager->NumRegionInUse();
  LOG(kInfo) << "Conc GC: " << nr_evaced << " evacuated, " << nr_reclaimed
             << " reclaimed ("
             << std::chrono::duration<double>(end - stt).count() << "s).";

  return nr_reclaimed;
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
  delete[] tasks;

  allocator->cleanup_regions();
  auto curr_nr_regions = regions.size();

  LOG(kError) << "Before evacuation: " << prev_nr_regions << " regions";
  LOG(kError) << "After  evacuation: " << curr_nr_regions << " regions";
}

void Evacuator::scan(int nr_thds) {
  if (under_pressure_ > 0)
    return;
  std::unique_lock<std::mutex> ul(mtx_);

  auto allocator = LogAllocator::global_allocator();
  auto &regions = allocator->vRegions_;

  parallelizer<decltype(regions), std::shared_ptr<LogRegion>>(
      nr_thds, regions, [&](std::shared_ptr<LogRegion> region) {
        if (under_pressure_ > 0)
          return false;
        scan_region(region.get(), true);
        return true;
      });
}

template <class C, class T>
void Evacuator::parallelizer(int nr_workers, C &work_container,
                             std::function<bool(T)> fn) {
  auto *tasks = new std::vector<T>[nr_workers];
  int tid = 0;
  for (auto task : work_container) {
    tasks[tid].push_back(task);
    tid = (tid + 1) % nr_workers;
  }

  std::vector<std::thread> gc_thds;
  for (tid = 0; tid < nr_workers; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto region : tasks[tid]) {
        if (!fn(region))
          break;
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  delete[] tasks;
}

inline void Evacuator::scan_region(LogRegion *region, bool deactivate) {
  if (!region->sealed_)
    return;
  region->alive_bytes_ = 0;
  for (auto &chunk : region->vLogChunks_) {
    scan_chunk(chunk.get(), deactivate);
  }
}

inline bool Evacuator::evac_region(LogRegion *region) {
  if (!region->sealed_)
    return false;
  bool ret = true;
  for (auto &chunk : region->vLogChunks_) {
    ret &= evac_chunk(chunk.get());
  }
  if (ret)
    region->destroy();
  return ret;
}

inline bool Evacuator::free_region(LogRegion *region) {
  if (!region->sealed_)
    return false;
  bool ret = true;
  for (auto &chunk : region->vLogChunks_) {
    ret &= free_chunk(chunk.get());
  }
  if (ret)
    region->destroy();
  return ret;
}

/** Evacuate a particular chunk */
inline bool Evacuator::iterate_chunk(LogChunk *chunk, uint64_t &pos,
                                     ObjectPtr &optr) {
  if (pos + sizeof(MetaObjectHdr) > chunk->pos_)
    return false;

  auto ret = optr.init_from_soft(TransientPtr(pos, sizeof(MetaObjectHdr)));
  if (ret == RetCode::Fail)
    return false;
  else if (ret == RetCode::Fault) {
    LOG(kError) << "chunk is unmapped under the hood";
    return false;
  }
  assert(ret == RetCode::Succ);
  if (optr.is_small_obj())
    pos += optr.total_size();
  else {
    LOG(kError) << "Not implemented yet!";
    exit(-1);
    // TODO: adapt to large obj spanning multiple chunks
    // pos += optr.total_size();
  }
  return true;
}

inline bool Evacuator::scan_chunk(LogChunk *chunk, bool deactivate) {
  if (!chunk->sealed_)
    return false;
  chunk->alive_bytes_ = 0;

  int nr_deactivated = 0;
  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

  ObjectPtr obj_ptr;

  auto pos = chunk->start_addr_;
  while (iterate_chunk(chunk, pos, obj_ptr)) {
    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;

      auto obj_size = obj_ptr.total_size();
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_present()) {
          if (!deactivate) {
            chunk->upd_alive_bytes(obj_size);
          } else if (meta_hdr.is_accessed()) {
            meta_hdr.dec_accessed();
            if (!store_hdr(meta_hdr, obj_ptr))
              goto faulted;
            nr_deactivated++;
            chunk->upd_alive_bytes(obj_size);
          } else {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          }
        } else
          nr_non_present++;
      }
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
              << ", nr_non_present: " << nr_non_present
              << ", nr_deactivated: " << nr_deactivated
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed
              << ", alive ratio: "
              << static_cast<float>(chunk->alive_bytes_) / kLogChunkSize;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

inline bool Evacuator::evac_chunk(LogChunk *chunk) {
  if (!chunk->sealed_)
    return false;

  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

  ObjectPtr obj_ptr;
  auto pos = chunk->start_addr_;
  while (iterate_chunk(chunk, pos, obj_ptr)) {
    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;

      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_present()) {
          nr_present++;
          obj_ptr.unlock(lock_id);
          auto allocator = LogAllocator::global_allocator();
          auto optptr = allocator->alloc_(obj_ptr.data_size(), true);
          lock_id = optptr->lock();
          assert(lock_id != -1 && !obj_ptr.null());

          if (optptr) {
            auto new_ptr = *optptr;
            auto ret = new_ptr.move_from(obj_ptr);
            if (ret == RetCode::Succ) {
              nr_moved++;
            } else if (ret == RetCode::Fail) {
              LOG(kError) << "Failed to move the object!";
              nr_failed++;
            } else
              goto faulted;
          } else
            nr_failed++;
        } else
          nr_freed++;
      }
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
          goto faulted;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_present: " << nr_present << ", nr_moved: " << nr_moved
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

inline bool Evacuator::free_chunk(LogChunk *chunk) {
  if (!chunk->sealed_)
    return false;

  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

  ObjectPtr obj_ptr;
  auto pos = chunk->start_addr_;
  while (iterate_chunk(chunk, pos, obj_ptr)) {
    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;

      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_present()) {
          if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
            goto faulted;
          nr_freed++;
        } else
          nr_non_present++;
      }
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
          goto faulted;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_freed: " << nr_freed
              << ", nr_non_present: " << nr_non_present
              << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

} // namespace cachebank
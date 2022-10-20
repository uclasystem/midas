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

namespace cachebank {

int64_t Evacuator::stw_gc(int64_t nr_to_reclaim) {
  if (__sync_fetch_and_add(&under_pressure_, 1) > 0) {
    std::unique_lock<std::mutex> ul(mtx_);
    __sync_fetch_and_add(&under_pressure_, -1);
    return 1;
  }

  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  std::atomic_int64_t nr_reclaimed = 0;
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
        scan_region(region, false);
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
        if (evac_region(region))
          nr_reclaimed++;
        if (nr_reclaimed >= nr_to_reclaim)
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

  if (nr_reclaimed < nr_to_reclaim) {
    for (auto &region : regions) {
      if (free_region(region.get()))
        nr_reclaimed++;
      if (nr_reclaimed >= nr_to_reclaim)
        break;
    }
    allocator->cleanup_regions();
    curr_nr_regions = regions.size();
  }

  auto end = std::chrono::steady_clock::now();

  LOG(kInfo) << "STW GC: reclaimed " << nr_reclaimed << " regions ("
             << std::chrono::duration<double>(end - stt).count() << "s).";

  __sync_fetch_and_add(&under_pressure_, -1);
  return prev_nr_regions - curr_nr_regions;
}

int64_t Evacuator::conc_gc(int nr_thds) {
  constexpr static float kAliveThresholdLow = 0.6;
  constexpr static float kAliveThresholdHigh = 0.9;
  static float kAliveThreshold = kAliveThresholdLow;

  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = std::chrono::steady_clock::now();
  std::atomic_int64_t nr_reclaimed = 0;
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
        scan_region(region, true);
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
        if (evac_region(region))
          nr_reclaimed++;
      }
    }));
  }
  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] evac_tasks;

  allocator->cleanup_regions();

done:
  auto end = std::chrono::steady_clock::now();
  curr_nr_regions = regions.size();
  LOG(kInfo) << "Conc GC: reclaimed " << nr_reclaimed << " regions ("
             << std::chrono::duration<double>(end - stt).count() << "s).";

  return nr_reclaimed_regions;
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
        scan_region(region, true);
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
#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "timer.hpp"
#include "utils.hpp"

namespace cachebank {

inline bool scannable() {
  const auto total_accessed = LogAllocator::total_access_cnt();
  const auto total_alive = LogAllocator::total_alive_cnt();
  if (total_alive == 0 || total_accessed <= total_alive * kGCScanFreqFactor)
    return false;
  LOG(kError) << "total acc cnt: " << total_accessed
              << ", total alive cnt: " << total_alive;
  LogAllocator::reset_access_cnt();
  LogAllocator::reset_alive_cnt();
  return true;
}

inline std::pair<int64_t, float> get_nr_to_reclaim() {
  auto manager = ResourceManager::global_manager();
  float avail_ratio = static_cast<float>(manager->NumRegionAvail() + 1) /
                      (manager->NumRegionLimit() + 1);
  uint64_t nr_to_reclaim = 0;
  if (avail_ratio < 0.05)
    nr_to_reclaim = std::max(manager->NumRegionLimit() / 100, 2ul);
  else if (avail_ratio < 0.1)
    nr_to_reclaim = std::max(manager->NumRegionLimit() / 200, 1ul);
  else if (avail_ratio < 0.2)
    nr_to_reclaim = manager->NumRegionLimit() / 800;
  else
    nr_to_reclaim = 0;
  return std::make_pair(nr_to_reclaim, avail_ratio);
}

void Evacuator::init() {
  scanner_thd_ = std::make_shared<std::thread>([&]() {
    while (!terminated_) {
      {
        std::unique_lock lk(scanner_mtx_);
        scanner_cv_.wait_for(lk, std::chrono::milliseconds(60 * 1000),
                             [this] { return terminated_ || scannable(); });
      }
      scan(kNumScanThds);
    }
  });

  evacuator_thd_ = std::make_shared<std::thread>([&]() {
    while (!terminated_) {
      {
        std::unique_lock lk(evacuator_mtx_);
        evacuator_cv_.wait(lk, [this] {
          auto [nr_to_reclaim, _] = get_nr_to_reclaim();
          return terminated_ || nr_to_reclaim;
        });
      }
      gc();
    }
  });
}

void Evacuator::scan(int nr_thds) {
  // if (!scannable())
  //   return;

  std::unique_lock<std::mutex> ul(mtx_);
  auto allocator = LogAllocator::global_allocator();
  auto &segments = allocator->vSegments_;
  std::mutex evac_mtx;
  std::vector<LogSegment *> agg_evac_tasks;

  parallelizer(nr_thds, segments,
               std::function([&](std::shared_ptr<LogSegment> segment) {
                 if (under_pressure_ > 0)
                   return false;
                 scan_segment(segment.get(), true);
                 if (segment->get_alive_ratio() < kAliveThresholdLow) {
                   std::unique_lock<std::mutex> ul(evac_mtx);
                   agg_evac_tasks.push_back(segment.get());
                 }
                 return true;
               }));
  if (agg_evac_tasks.empty()) {
    return;
  }
  // std::unique_lock<std::mutex> ul(mtx_);
  for (auto segment : agg_evac_tasks) {
    evac_segment(segment);
  }
  allocator->cleanup_segments();
}

int64_t Evacuator::gc() {
  auto [nr_to_reclaim, avail_region_ratio] = get_nr_to_reclaim();
  if (nr_to_reclaim == 0)
    return 0;

  auto stt = timer::timer();
  auto allocator = LogAllocator::global_allocator();
  std::atomic_int64_t nr_evaced = 0;
  auto nr_evac_thds = nr_to_reclaim;
  std::vector<LogSegment *> agg_evac_tasks;

retry:
  std::unique_lock<std::mutex> ul(mtx_);
  auto &segments = allocator->vSegments_;
  if (segments.size() == 0) {
    LOG(kError) << segments.size();
    return 0;
  }

  for (auto segment : segments) {
    auto alive_ratio = segment->get_alive_ratio();
    if (alive_ratio < kGCEvacThreshold)
      agg_evac_tasks.push_back(segment.get());
  }
  if (agg_evac_tasks.empty()) {
    if (avail_region_ratio > 0.1) { // TODO (YIFAN): this is dirty
      LOG(kError) << avail_region_ratio;
      return 0;
    }
    else {
      ul.unlock();
      scan();
      goto retry;
    }
  }

  parallelizer(nr_evac_thds, agg_evac_tasks,
               std::function([&](LogSegment *segment) {
                 if (evac_segment(segment))
                   nr_evaced++;
                 return true;
               }));
  agg_evac_tasks.clear();
  allocator->cleanup_segments();

  auto end = timer::timer();
  auto nr_reclaimed = ResourceManager::global_manager()->NumRegionAvail();
  LOG(kError) << "GC: " << nr_evaced << " evacuated, " << nr_reclaimed
             << " reclaimed (" << timer::duration(stt, end) << "s).";

  return nr_reclaimed;
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
      for (auto segment : tasks[tid]) {
        if (!fn(segment))
          break;
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  delete[] tasks;
}

inline bool Evacuator::segment_ready(LogSegment *segment) {
  if (!segment->sealed_ || segment->destroyed())
    return false;
  bool sealed = true;
  for (auto &chunk : segment->vLogChunks_) {
    if (!chunk->sealed_) {
      sealed = false;
      break;
    }
  }
  if (!sealed)
    return false;
  return true;
}

inline void Evacuator::scan_segment(LogSegment *segment, bool deactivate) {
  if (!segment_ready(segment))
    return;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return;

  int32_t alive_bytes = 0;
  for (auto &chunk : segment->vLogChunks_) {
    alive_bytes += scan_chunk(chunk.get(), deactivate);
  }
  segment->alive_bytes_ = alive_bytes;
}

inline bool Evacuator::evac_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return false;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return false;

  bool ret = true;
  for (auto &chunk : segment->vLogChunks_) {
    ret &= evac_chunk(chunk.get());
  }
  if (ret)
    segment->destroy();
  return ret;
}

inline bool Evacuator::free_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return false;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return false;

  bool ret = true;
  for (auto &chunk : segment->vLogChunks_) {
    ret &= free_chunk(chunk.get());
  }
  if (ret)
    segment->destroy();
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
    pos += optr.obj_size();
  else { // large obj
    pos += optr.obj_size(); // TODO: the size here is incorrect!
  }
  return true;
}

inline int32_t Evacuator::scan_chunk(LogChunk *chunk, bool deactivate) {
  if (!chunk->sealed_)
    return kMaxAliveBytes;

  int alive_bytes = 0;
  int nr_present = 0;

  // counters
  int nr_deactivated = 0;
  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_large_objs = 0;
  int nr_failed = 0;

  ObjectPtr obj_ptr;

  auto pos = chunk->start_addr_;
  while (iterate_chunk(chunk, pos, obj_ptr)) {
    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;

      auto obj_size = obj_ptr.obj_size();
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_present()) {
          nr_present++;
          if (!deactivate) {
            alive_bytes += obj_size;
          } else if (meta_hdr.is_accessed()) {
            meta_hdr.dec_accessed();
            if (!store_hdr(meta_hdr, obj_ptr))
              goto faulted;
            nr_deactivated++;
            alive_bytes += obj_size;
          } else {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          }
        } else
          nr_non_present++;
      }
    } else { // large object
      // ABORT("Not implemented yet!");
      nr_large_objs++;
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        auto obj_size = obj_ptr.obj_size(); // TODO: incorrect size here!
        if (meta_hdr.is_present()) {
          nr_present++;
          if (!meta_hdr.is_continue()) { // head chunk
            if (!deactivate) {
              alive_bytes += obj_size;
            } else if (meta_hdr.is_accessed()) {
              meta_hdr.dec_accessed();
              if (!store_hdr(meta_hdr, obj_ptr))
                goto faulted;
              nr_deactivated++;
              alive_bytes += obj_size;
            } else {
              // This will free all chunks belonging to this object
              if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
                goto faulted;

              nr_freed++;
            }
          } else { // continued chunk
            ABORT("Impossible for now");
            // this is a inner chunk storing a large object.
            alive_bytes += obj_size;
          }
        } else
          nr_non_present++;
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
  if (nr_failed) {
    ABORT("TODO: fault-aware");
    return kMaxAliveBytes;
  }

  chunk->set_alive_bytes(alive_bytes);
  if (deactivate) // meaning this is a scanning thread
    LogAllocator::count_alive(nr_present);
  LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
              << ", nr_large_objs: " << nr_large_objs
              << ", nr_non_present: " << nr_non_present
              << ", nr_deactivated: " << nr_deactivated
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed
              << ", alive ratio: "
              << static_cast<float>(chunk->alive_bytes_) / kLogChunkSize;

  return alive_bytes;
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
    MetaObjectHdr meta_hdr;
    if (!load_hdr(meta_hdr, obj_ptr))
      goto faulted;
    if (!meta_hdr.is_present()) {
      nr_freed++;
      obj_ptr.unlock(lock_id);
      continue;
    }
    nr_present++;
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;
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
    } else { // large object
      if (!meta_hdr.is_continue()) { // the head chunk of a large object.
        obj_ptr.unlock(lock_id);
        auto allocator = LogAllocator::global_allocator();
        auto optptr = allocator->alloc_(obj_ptr.data_size(), true);
        lock_id = obj_ptr.lock();
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
      } else { // an inner chunk of a large obj
        // TODO: remap this chunk directly if (obj_size == kLogChunkSize).
        /* For now, let's skip it and let the head chunk handle all the
         * continued chunks together. */
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
    } else { // large object
      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (!meta_hdr.is_continue()) {
          // this is the head chunk of a large object.
          if (meta_hdr.is_present()) {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          } else
            nr_non_present++;
        } else {
          // this is a inner chunk storing a large object.
          ABORT("Not implemented yet!");
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

/** [Deprecated] */

int64_t Evacuator::stw_gc(int64_t nr_to_reclaim) {
  if (__sync_fetch_and_add(&under_pressure_, 1) > 0) {
    std::unique_lock<std::mutex> ul(mtx_);
    __sync_fetch_and_add(&under_pressure_, -1);
    return 1;
  }
  std::unique_lock<std::mutex> ul(mtx_);

  auto stt = timer::timer();
  std::atomic_int64_t nr_evaced = 0;
  auto rmanager = ResourceManager::global_manager();
  auto allocator = LogAllocator::global_allocator();
  auto &segments = allocator->vSegments_;
  auto nr_scan_thds = nr_gc_thds_;
  auto nr_evac_thds = 1;

  auto prev_nr_segments = segments.size();

  using Pair = std::pair<float, LogSegment *>;
  std::mutex evac_mtx;
  std::vector<Pair> agg_evac_tasks;

  parallelizer(nr_scan_thds, segments,
               std::function([&](std::shared_ptr<LogSegment> segment) {
                 auto segment_ = segment.get();
                 scan_segment(segment_, false);
                 auto alive_ratio = segment_->get_alive_ratio();
                 if (alive_ratio > kAliveThresholdHigh)
                   return true;
                 std::unique_lock<std::mutex> ul(evac_mtx);
                 agg_evac_tasks.push_back(
                     std::make_pair(alive_ratio, segment_));
                 return true;
               }));

  if (!agg_evac_tasks.empty()) {
    std::sort(agg_evac_tasks.begin(), agg_evac_tasks.end(),
              [](Pair v1, Pair v2) { return v1.first < v2.first; });
    parallelizer(nr_evac_thds, agg_evac_tasks, std::function([&](Pair p) {
                   if (evac_segment(p.second))
                     nr_evaced++;
                   return rmanager->NumRegionAvail() < nr_to_reclaim;
                 }));
    agg_evac_tasks.clear();
    allocator->cleanup_segments();
  }

  if (rmanager->NumRegionAvail() < nr_to_reclaim) {
    for (auto &segment : segments) {
      if (free_segment(segment.get()))
        nr_evaced++;
      auto rmanager = ResourceManager::global_manager();
      if (rmanager->NumRegionAvail() >= nr_to_reclaim)
        break;
    }
    allocator->cleanup_segments();
  }

  auto end = timer::timer();
  __sync_fetch_and_add(&under_pressure_, -1);

  auto nr_reclaimed = rmanager->NumRegionAvail();
  LOG(kInfo) << "STW GC: " << nr_evaced << " evacuated, " << nr_reclaimed
             << " reclaimed (" << timer::duration(stt, end) << "s).";
  return nr_reclaimed;
}

int64_t Evacuator::conc_gc(int nr_thds) {
  static float kAliveThreshold = kAliveThresholdLow;

  std::unique_lock<std::mutex> ul(mtx_);

  if (!scannable())
    return 0;

  auto stt = timer::timer();
  std::atomic_int64_t nr_evaced = 0;
  auto allocator = LogAllocator::global_allocator();
  auto &segments = allocator->vSegments_;
  auto nr_scan_thds = nr_thds;
  auto nr_evac_thds = 1;

  std::vector<std::thread> gc_thds;
  // auto *scan_tasks = new std::vector<LogSegment *>[nr_scan_thds];
  using Pair = std::pair<float, LogSegment *>;
  std::mutex evac_mtx;
  std::vector<Pair> agg_evac_tasks;
  auto *evac_tasks = new std::vector<LogSegment *>[nr_evac_thds];

  int tid = 0;
  parallelizer(nr_scan_thds, segments,
               std::function([&](std::shared_ptr<LogSegment> segment) {
                 auto segment_ = segment.get();
                 scan_segment(segment_, true);
                 auto alive_ratio = segment->get_alive_ratio();
                 if (alive_ratio <= kAliveThreshold) {
                   std::unique_lock<std::mutex> ul(evac_mtx);
                   agg_evac_tasks.push_back(
                       std::make_pair(alive_ratio, segment_));
                 }
                 return true;
               }));

  if (agg_evac_tasks.empty()) {
    kAliveThreshold =
        std::min<float>(kAliveThreshold + kAliveIncStep, kAliveThresholdHigh);
    goto done;
  } else {
    kAliveThreshold = kAliveThresholdLow;
  }
  std::sort(agg_evac_tasks.begin(), agg_evac_tasks.end(),
            [](Pair v1, Pair v2) { return v1.first < v2.first; });

  parallelizer(nr_evac_thds, agg_evac_tasks, std::function([&](Pair p) {
                 if (evac_segment(p.second))
                   nr_evaced++;
                 return true;
               }));
  agg_evac_tasks.clear();
  allocator->cleanup_segments();

done:
  auto end = timer::timer();
  auto rmanager = ResourceManager::global_manager();
  auto nr_reclaimed = rmanager->NumRegionAvail();
  LOG(kInfo) << "Conc GC: " << nr_evaced << " evacuated, " << nr_reclaimed
             << " reclaimed (" << timer::duration(stt, end) << "s).";

  return nr_reclaimed;
}

void Evacuator::evacuate(int nr_thds) {
  if (under_pressure_ > 0)
    return;
  std::unique_lock<std::mutex> ul(mtx_);

  auto allocator = LogAllocator::global_allocator();
  auto &segments = allocator->vSegments_;

  std::vector<std::thread> gc_thds;
  auto *tasks = new std::vector<LogSegment *>[nr_thds];

  int tid = 0;
  auto prev_nr_segments = segments.size();
  for (auto segment : segments) {
    auto raw_ptr = segment.get();
    if (raw_ptr) {
      tasks[tid].push_back(raw_ptr);
      tid = (tid + 1) % nr_thds;
    }
  }

  for (tid = 0; tid < nr_thds; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      for (auto segment : tasks[tid]) {
        if (under_pressure_ > 0)
          break;
        evac_segment(segment);
      }
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();
  delete[] tasks;

  allocator->cleanup_segments();
  auto curr_nr_segments = segments.size();

  LOG(kError) << "Before evacuation: " << prev_nr_segments << " regions";
  LOG(kError) << "After  evacuation: " << curr_nr_segments << " regions";
}

} // namespace cachebank
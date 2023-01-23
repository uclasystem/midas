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

static inline int64_t get_nr_to_reclaim() {
  auto manager = ResourceManager::global_manager();
  float avail_ratio = static_cast<float>(manager->NumRegionAvail() + 1) /
                      (manager->NumRegionLimit() + 1);
  uint64_t nr_to_reclaim = 0;
  if (avail_ratio < 0.05)
    nr_to_reclaim = std::max(manager->NumRegionLimit() / 1000, 2ul);
  else if (avail_ratio < 0.1)
    nr_to_reclaim = std::max(manager->NumRegionLimit() / 2000, 1ul);
  else if (avail_ratio < 0.2)
    nr_to_reclaim = manager->NumRegionLimit() / 8000;
  else
    nr_to_reclaim = 0;
  return nr_to_reclaim;
}

void Evacuator::init() {
  gc_thd_ = std::make_shared<std::thread>([&]() {
    while (!terminated_) {
      {
        std::unique_lock lk(gc_mtx_);
        gc_cv_.wait(lk, [this] { return terminated_ || get_nr_to_reclaim(); });
      }
      // parallel_gc(1);
      serial_gc();
    }
  });
}

int64_t Evacuator::gc(SegmentList &stash_list) {
  auto allocator = LogAllocator::global_allocator();
  auto rmanager = ResourceManager::global_manager();
  auto nr_target = get_nr_to_reclaim();
  auto nr_avail = rmanager->NumRegionAvail();
  if (nr_avail >= nr_target)
    return 0;

  std::atomic_int64_t nr_scanned = 0;
  std::atomic_int64_t nr_evaced = 0;
  auto &segments = allocator->segments_;

  auto stt = timer::timer();
  while (rmanager->NumRegionAvail() < nr_target) {
    auto segment = segments.pop_front();
    if (!segment)
      continue;
    EvacState ret = scan_segment(segment.get(), true);
    if (ret == EvacState::Fault)
      continue;
    else if (ret == EvacState::Fail)
      goto put_back;
    // must have ret == EvacState::Succ now
    nr_scanned++;

    if (segment->get_alive_ratio() >= kAliveThreshHigh)
      goto put_back;
    ret = evac_segment(segment.get());
    if (ret == EvacState::Fail)
      goto put_back;
    else if (ret == EvacState::DelayRelease)
      goto stash;
    // must have ret == EvacState::Succ or ret == EvacState::Fault now
    nr_evaced++;
    continue;
  put_back:
    segments.push_back(segment);
    continue;
  stash:
    stash_list.push_back(segment);
    continue;
  }
  auto end = timer::timer();

  auto nr_reclaimed = rmanager->NumRegionAvail() - nr_avail;

  if (nr_scanned)
    LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                << " evacuated, " << nr_reclaimed << " reclaimed ("
                << timer::duration(stt, end) << "s).";

  return nr_reclaimed;
}

int64_t Evacuator::serial_gc() {
  auto allocator = LogAllocator::global_allocator();
  auto rmanager = ResourceManager::global_manager();
  auto nr_target = get_nr_to_reclaim();
  auto nr_avail = rmanager->NumRegionAvail();
  if (nr_avail >= nr_target)
    return 0;

  std::atomic_int64_t nr_scanned = 0;
  std::atomic_int64_t nr_evaced = 0;
  auto &segments = allocator->segments_;

  auto stt = timer::timer();
  while (rmanager->NumRegionAvail() < nr_target) {
    auto segment = segments.pop_front();
    if (!segment)
      continue;
    EvacState ret = scan_segment(segment.get(), true);
    if (ret == EvacState::Fault)
      continue;
    else if (ret == EvacState::Fail)
      goto put_back;
    // must have ret == EvacState::Succ now
    nr_scanned++;

    if (segment->get_alive_ratio() >= kAliveThreshHigh)
      goto put_back;
    ret = evac_segment(segment.get());
    if (ret == EvacState::Fail)
      goto put_back;
    else if (ret == EvacState::DelayRelease) {
      LOG(kWarning);
      segment->destroy();
    }
    // must have ret == EvacState::Succ or ret == EvacState::Fault now
    nr_evaced++;
    continue;
  put_back:
    segments.push_back(segment);
    continue;
  }
  auto end = timer::timer();

  auto nr_reclaimed = rmanager->NumRegionAvail() - nr_avail;

  if (nr_scanned)
    LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                << " evacuated, " << nr_reclaimed << " reclaimed ("
                << timer::duration(stt, end) << "s).";

  return nr_reclaimed;
}

void Evacuator::parallel_gc(int nr_workers) {
  SegmentList stash_list;

  std::vector<std::thread> gc_thds;
  for (int tid = 0; tid < nr_workers; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      gc(stash_list);
    }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  auto &segments = LogAllocator::global_allocator()->segments_;
  while (!stash_list.empty()) {
    auto segment = stash_list.pop_front();
    // segment->destroy();
    EvacState ret = evac_segment(segment.get());
    if (ret == EvacState::DelayRelease)
      segments.push_back(segment);
    else if (ret != EvacState::Succ) {
      LOG(kError) << (int)ret;
      segments.push_back(segment);
    }
  }
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

inline EvacState Evacuator::scan_segment(LogSegment *segment, bool deactivate) {
  if (!segment_ready(segment))
    return EvacState::Fail;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return EvacState::Fail;

  assert(segment->vLogChunks_.size() == 1);
  auto chunk = segment->vLogChunks_.front();
  EvacState ret = scan_chunk(chunk.get(), deactivate);
  if (ret == EvacState::Fault)
    segment->destroy();
  segment->alive_bytes_ = chunk->alive_bytes_;
  return ret;
}

inline EvacState Evacuator::evac_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return EvacState::Fail;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return EvacState::Fail;

  assert(segment->vLogChunks_.size() == 1);
  auto chunk = segment->vLogChunks_.front();
  EvacState ret = evac_chunk(chunk.get());
  if (ret == EvacState::Succ || ret == EvacState::Fault)
    segment->destroy();
  return ret;
}

inline EvacState Evacuator::free_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return EvacState::Fail;
  std::unique_lock<std::mutex> ul(segment->mtx_, std::defer_lock);
  if (!ul.try_lock())
    return EvacState::Fail;

  assert(segment->vLogChunks_.size() == 1);
  auto chunk = segment->vLogChunks_.front();
  EvacState ret = free_chunk(chunk.get());
  if (ret == EvacState::Succ || ret == EvacState::Fault)
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
  pos += optr.obj_size(); // header size is already counted
  return true;
}

inline EvacState Evacuator::scan_chunk(LogChunk *chunk, bool deactivate) {
  chunk->set_alive_bytes(kMaxAliveBytes);
  if (!chunk->sealed_)
    return EvacState::Fail;

  int alive_bytes = 0;
  // counters
  int nr_present = 0;
  int nr_deactivated = 0;
  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_large_objs = 0;
  int nr_failed = 0;
  int nr_contd_objs = 0;

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
              // This will free all chunks belonging to the same object
              if (obj_ptr.free(/* locked = */ true) == RetCode::Fault) {
                LOG(kWarning);
                // goto faulted;
              }

              nr_freed++;
            }
          } else { // continued chunk
            // An inner chunk of a large object. Skip it.
            alive_bytes += obj_size;
            nr_contd_objs++;
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

  assert(nr_failed == 0);
  if (deactivate) // meaning this is a scanning thread
    LogAllocator::count_alive(nr_present);
  LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
              << ", nr_large_objs: " << nr_large_objs
              << ", nr_non_present: " << nr_non_present
              << ", nr_deactivated: " << nr_deactivated
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed
              << ", alive ratio: "
              << static_cast<float>(chunk->alive_bytes_) / kLogChunkSize;

  if (nr_failed) {
    return EvacState::Fault;
  }
  chunk->set_alive_bytes(alive_bytes);
  return EvacState::Succ;
}

inline EvacState Evacuator::evac_chunk(LogChunk *chunk) {
  if (!chunk->sealed_)
    return EvacState::Fail;

  // counters
  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;
  int nr_contd_objs = 0;

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
      auto optptr = allocator->alloc_(obj_ptr.data_size_in_chunk(), true);
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
    } else {                         // large object
      if (!meta_hdr.is_continue()) { // the head chunk of a large object.
        auto opt_data_size = obj_ptr.large_data_size();
        if (!opt_data_size) {
          LOG(kWarning);
          nr_freed++;
          obj_ptr.unlock(lock_id);
          continue;
        }
        obj_ptr.unlock(lock_id);
        auto allocator = LogAllocator::global_allocator();
        auto optptr = allocator->alloc_(*opt_data_size, true);
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
          } else { // ret == RetCode::Fault
            LOG(kWarning);
            // goto faulted;
          }
        } else
          nr_failed++;
      } else { // an inner chunk of a large obj
        // TODO: remap this chunk directly if (obj_size == kLogChunkSize).
        /* For now, let's skip it and let the head chunk handle all the
         * continued chunks together.
         */
        nr_contd_objs++;
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
  if (nr_failed)
    return EvacState::Fault;
  if (nr_contd_objs)
    return EvacState::DelayRelease;
  return EvacState::Succ;
}

inline EvacState Evacuator::free_chunk(LogChunk *chunk) {
  if (!chunk->sealed_)
    return EvacState::Fail;

  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;
  int nr_contd_objs = 0;

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
        if (!meta_hdr.is_continue()) { // head chunk
          if (meta_hdr.is_present()) {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          } else
            nr_non_present++;
        } else { // continued chunk
          nr_contd_objs++;
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
  if (nr_failed)
    return EvacState::Fault;
  if (nr_contd_objs)
    return EvacState::DelayRelease;
  return EvacState::Succ;
}

} // namespace cachebank
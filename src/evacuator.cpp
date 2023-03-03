#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "cache_manager.hpp"
#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "time.hpp"
#include "utils.hpp"

namespace midas {

static inline int64_t get_nr_to_reclaim() {
  auto manager = ResourceManager::global_manager();
  float avail_ratio = static_cast<float>(manager->NumRegionAvail() + 1) /
                      (manager->NumRegionLimit() + 1);
  uint64_t nr_to_reclaim = 0;
  if (manager->NumRegionLimit() - manager->NumRegionAvail() >= 1)
    nr_to_reclaim = std::max(manager->NumRegionLimit() / 1000, 1ul);
  else
    nr_to_reclaim = 0;
  return nr_to_reclaim;
}

void Evacuator::init() {
  rmanager_ = ResourceManager::global_manager_shared_ptr();
  gc_thd_ = std::make_shared<std::thread>([&]() {
    while (!terminated_) {
      {
        std::unique_lock<std::mutex> lk(gc_mtx_);
        gc_cv_.wait(lk, [this] { return terminated_ || get_nr_to_reclaim(); });
      }
      parallel_gc(8);
      // serial_gc();
    }
  });
}

int64_t Evacuator::gc(SegmentList &stash_list) {
  auto nr_target = get_nr_to_reclaim();
  auto nr_avail = rmanager_->NumRegionAvail();
  if (nr_avail >= nr_target)
    return 0;

  std::atomic_int64_t nr_scanned = 0;
  std::atomic_int64_t nr_evaced = 0;
  auto &segments = allocator_->segments_;

  auto stt = chrono_utils::now();
  while (rmanager_->NumRegionAvail() < nr_target) {
    auto segment = segments.pop_front();
    if (!segment)
      continue;
    if (!segment->sealed()) { // put in-used segment back to list
      segments.push_back(segment);
      continue;
    }
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
  auto end = chrono_utils::now();

  auto nr_reclaimed = rmanager_->NumRegionAvail() - nr_avail;

  if (nr_scanned)
    MIDAS_LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                      << " evacuated, " << nr_reclaimed << " reclaimed ("
                      << chrono_utils::duration(stt, end) << "s).";

  return nr_reclaimed;
}

int64_t Evacuator::serial_gc() {
  auto nr_target = get_nr_to_reclaim();
  auto nr_avail = rmanager_->NumRegionAvail();
  if (nr_avail >= nr_target)
    return 0;

  std::atomic_int64_t nr_scanned = 0;
  std::atomic_int64_t nr_evaced = 0;
  auto &segments = allocator_->segments_;

  auto stt = chrono_utils::now();
  while (rmanager_->NumRegionAvail() < nr_target) {
    auto segment = segments.pop_front();
    if (!segment)
      continue;
    if (!segment->sealed()) { // put in-used segment back to list
      segments.push_back(segment);
      continue;
    }
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
      MIDAS_LOG(kWarning);
      segment->destroy();
    }
    // must have ret == EvacState::Succ or ret == EvacState::Fault now
    nr_evaced++;
    continue;
  put_back:
    segments.push_back(segment);
    continue;
  }
  auto end = chrono_utils::now();

  auto nr_reclaimed = rmanager_->NumRegionAvail() - nr_avail;

  if (nr_scanned)
    MIDAS_LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                      << " evacuated, " << nr_reclaimed << " reclaimed ("
                      << chrono_utils::duration(stt, end) << "s).";

  return nr_reclaimed;
}

void Evacuator::parallel_gc(int nr_workers) {
  SegmentList stash_list;

  std::vector<std::thread> gc_thds;
  for (int tid = 0; tid < nr_workers; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() { gc(stash_list); }));
  }

  for (auto &thd : gc_thds)
    thd.join();
  gc_thds.clear();

  auto &segments = allocator_->segments_;
  while (!stash_list.empty()) {
    auto segment = stash_list.pop_front();
    // segment->destroy();
    EvacState ret = evac_segment(segment.get());
    if (ret == EvacState::DelayRelease)
      segments.push_back(segment);
    else if (ret != EvacState::Succ) {
      MIDAS_LOG(kError) << (int)ret;
      segments.push_back(segment);
    }
  }
}

inline bool Evacuator::segment_ready(LogSegment *segment) {
  return segment->sealed_ && !segment->destroyed();
}

/** Evacuate a particular segment */
inline bool Evacuator::iterate_segment(LogSegment *segment, uint64_t &pos,
                                       ObjectPtr &optr) {
  if (pos + sizeof(MetaObjectHdr) > segment->pos_)
    return false;

  auto ret = optr.init_from_soft(TransientPtr(pos, sizeof(LargeObjectHdr)));
  if (ret == RetCode::Fail)
    return false;
  else if (ret == RetCode::Fault) {
    MIDAS_LOG(kError) << "segment is unmapped under the hood";
    return false;
  }
  assert(ret == RetCode::Succ);
  pos += optr.obj_size(); // header size is already counted
  return true;
}

inline EvacState Evacuator::scan_segment(LogSegment *segment, bool deactivate) {
  if (!segment_ready(segment))
    return EvacState::Fail;
  segment->set_alive_bytes(kMaxAliveBytes);

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

  auto pos = segment->start_addr_;
  while (iterate_segment(segment, pos, obj_ptr)) {
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
            auto vcache = pool_->get_vcache();
            vcache->push_back(obj_ptr.get_rref(), nullptr);
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
        auto obj_size = obj_ptr.obj_size(); // only partial size here!
        if (meta_hdr.is_present()) {
          nr_present++;
          if (!meta_hdr.is_continue()) { // head segment
            if (!deactivate) {
              alive_bytes += obj_size;
            } else if (meta_hdr.is_accessed()) {
              meta_hdr.dec_accessed();
              if (!store_hdr(meta_hdr, obj_ptr))
                goto faulted;
              nr_deactivated++;
              alive_bytes += obj_size;
            } else {
              // This will free all segments belonging to the same object
              if (obj_ptr.free(/* locked = */ true) == RetCode::Fault) {
                MIDAS_LOG(kWarning);
                // goto faulted;
              }
              auto vcache = pool_->get_vcache();
              vcache->push_back(obj_ptr.get_rref(), nullptr);

              nr_freed++;
            }
          } else { // continued segment
            // An inner segment of a large object. Skip it.
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
    MIDAS_LOG(kError) << "segment is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }

  assert(nr_failed == 0);
  if (deactivate) // meaning this is a scanning thread
    LogAllocator::count_alive(nr_present);
  MIDAS_LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
                    << ", nr_large_objs: " << nr_large_objs
                    << ", nr_non_present: " << nr_non_present
                    << ", nr_deactivated: " << nr_deactivated
                    << ", nr_freed: " << nr_freed
                    << ", nr_failed: " << nr_failed << ", alive ratio: "
                    << static_cast<float>(segment->alive_bytes_) /
                           kLogSegmentSize;

  if (nr_failed) {
    segment->destroy();
    return EvacState::Fault;
  }
  segment->set_alive_bytes(alive_bytes);
  return EvacState::Succ;
}

inline EvacState Evacuator::evac_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return EvacState::Fail;

  // counters
  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;
  int nr_contd_objs = 0;

  ObjectPtr obj_ptr;
  auto pos = segment->start_addr_;
  while (iterate_segment(segment, pos, obj_ptr)) {
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
      auto optptr = allocator_->alloc_(obj_ptr.data_size_in_segment(), true);
      lock_id = optptr->lock();
      assert(lock_id != -1 && !obj_ptr.null());

      if (optptr) {
        auto new_ptr = *optptr;
        auto ret = new_ptr.move_from(obj_ptr);
        if (ret == RetCode::Succ) {
          nr_moved++;
        } else if (ret == RetCode::Fail) {
          MIDAS_LOG(kError) << "Failed to move the object!";
          nr_failed++;
        } else
          goto faulted;
      } else
        nr_failed++;
    } else {                         // large object
      if (!meta_hdr.is_continue()) { // the head segment of a large object.
        auto opt_data_size = obj_ptr.large_data_size();
        if (!opt_data_size) {
          MIDAS_LOG(kWarning);
          nr_freed++;
          obj_ptr.unlock(lock_id);
          continue;
        }
        obj_ptr.unlock(lock_id);
        auto optptr = allocator_->alloc_(*opt_data_size, true);
        lock_id = obj_ptr.lock();
        assert(lock_id != -1 && !obj_ptr.null());

        if (optptr) {
          auto new_ptr = *optptr;
          auto ret = new_ptr.move_from(obj_ptr);
          if (ret == RetCode::Succ) {
            nr_moved++;
          } else if (ret == RetCode::Fail) {
            MIDAS_LOG(kError) << "Failed to move the object!";
            nr_failed++;
          } else { // ret == RetCode::Fault
            MIDAS_LOG(kWarning);
            // goto faulted;
          }
        } else
          nr_failed++;
      } else { // an inner segment of a large obj
        // TODO: remap this segment directly if (obj_size == kLogsegmentSize).
        /* For now, let's skip it and let the head segment handle all the
         * continued segments together.
         */
        nr_contd_objs++;
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    MIDAS_LOG(kError) << "segment is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  MIDAS_LOG(kDebug) << "nr_present: " << nr_present
                    << ", nr_moved: " << nr_moved << ", nr_freed: " << nr_freed
                    << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  if (nr_failed) {
    segment->destroy();
    return EvacState::Fault;
  }
  if (nr_contd_objs)
    return EvacState::DelayRelease;
  segment->destroy();
  return EvacState::Succ;
}

inline EvacState Evacuator::free_segment(LogSegment *segment) {
  if (!segment_ready(segment))
    return EvacState::Fail;

  // counters
  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;
  int nr_contd_objs = 0;

  ObjectPtr obj_ptr;
  auto pos = segment->start_addr_;
  while (iterate_segment(segment, pos, obj_ptr)) {
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
        if (!meta_hdr.is_continue()) { // head segment
          if (meta_hdr.is_present()) {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          } else
            nr_non_present++;
        } else { // continued segment
          nr_contd_objs++;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    MIDAS_LOG(kError) << "segment is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  MIDAS_LOG(kDebug) << "nr_freed: " << nr_freed
                    << ", nr_non_present: " << nr_non_present
                    << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  if (nr_failed) {
    segment->destroy();
    return EvacState::Fault;
  }
  if (nr_contd_objs)
    return EvacState::DelayRelease;
  segment->destroy();
  return EvacState::Succ;
}

} // namespace midas
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
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
void Evacuator::init() {
  gc_thd_ = std::make_shared<std::thread>([&]() {
    while (!terminated_) {
      {
        std::unique_lock<std::mutex> lk(gc_mtx_);
        gc_cv_.wait(
            lk, [this] { return terminated_ || rmanager_->reclaim_trigger(); });
      }
      auto succ = false;
      auto nr_evac_thds = rmanager_->reclaim_nr_thds();
      if (nr_evac_thds == 1) {
        succ = serial_gc();
      } else {
        for (int i = 0; i < 3; i++) {
          succ = parallel_gc(nr_evac_thds);
          if (succ)
            break;
        }
      }

      if (!succ)
        force_reclaim();
    }
  });
}

int64_t Evacuator::gc(SegmentList &stash_list) {
  if (!rmanager_->reclaim_trigger())
    return 0;

  int nr_skipped = 0;
  int nr_scanned = 0;
  int nr_evaced = 0;
  auto &segments = allocator_->segments_;

  auto stt = chrono_utils::now();
  while (rmanager_->reclaim_trigger()) {
    auto segment = segments.pop_front();
    if (!segment) {
      nr_skipped++;
      if (nr_skipped > rmanager_->NumRegionLimit())
        return -1;
      continue;
    }
    if (!segment->sealed()) { // put in-used segment back to list
      segments.push_back(segment);
      nr_skipped++;
      if (nr_skipped > rmanager_->NumRegionLimit()) { // be in loop for too long
        MIDAS_LOG(kDebug) << "Encountered too many unsealed segments during "
                             "GC, skip GC this round.";
        return -1;
      }
      continue;
    }
    EvacState ret = scan_segment(segment.get(), true);
    nr_scanned++;
    if (ret == EvacState::Fault)
      continue;
    else if (ret == EvacState::Fail)
      goto put_back;
    // must have ret == EvacState::Succ now

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

  auto nr_avail = rmanager_->NumRegionAvail();
  // assert(nr_avail > 0);

  if (nr_scanned)
    MIDAS_LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                      << " evacuated, " << nr_avail << " available ("
                      << chrono_utils::duration(stt, end) << "s).";

  return nr_avail;
}

bool Evacuator::serial_gc() {
  if (!rmanager_->reclaim_trigger())
    return 0;

  int64_t nr_skipped = 0;
  int64_t nr_scanned = 0;
  int64_t nr_evaced = 0;
  auto &segments = allocator_->segments_;

  auto stt = chrono_utils::now();
  while (rmanager_->reclaim_trigger()) {
    auto segment = segments.pop_front();
    if (!segment) {
      nr_skipped++;
      if (nr_skipped > rmanager_->NumRegionLimit()) // be in loop for too long
        goto done;
      continue;
    }
    if (!segment->sealed()) { // put in-used segment back to list
      segments.push_back(segment);
      nr_skipped++;
      if (nr_skipped > rmanager_->NumRegionLimit()) { // be in loop for too long
        MIDAS_LOG(kDebug) << "Encountered too many unsealed segments during "
                             "GC, skip GC this round.";
        goto done;
      }
      continue;
    }
    EvacState ret = scan_segment(segment.get(), true);
    nr_scanned++;
    if (ret == EvacState::Fault)
      continue;
    else if (ret == EvacState::Fail)
      goto put_back;
    // must have ret == EvacState::Succ now
    assert(ret == EvacState::Succ);

    if (segment->get_alive_ratio() >= kAliveThreshHigh)
      goto put_back;
    ret = evac_segment(segment.get());
    if (ret == EvacState::Fail)
      goto put_back;
    else if (ret == EvacState::DelayRelease) {
      // MIDAS_LOG(kWarning);
      segment->destroy();
    }
    // must have ret == EvacState::Succ or ret == EvacState::Fault now
    nr_evaced++;
    continue;
  put_back:
    segments.push_back(segment);
    continue;
  }

done:
  auto end = chrono_utils::now();
  auto nr_avail = rmanager_->NumRegionAvail();

  if (nr_scanned)
    MIDAS_LOG(kDebug) << "GC: " << nr_scanned << " scanned, " << nr_evaced
                      << " evacuated, " << nr_avail << " available ("
                      << chrono_utils::duration(stt, end) << "s).";

  return nr_avail >= 0;
}

bool Evacuator::parallel_gc(int nr_workers) {
  auto stt = chrono_utils::now();
  rmanager_->prof_reclaim_stt();

  SegmentList stash_list;
  std::atomic_int nr_failed{0};
  std::vector<std::thread> gc_thds;
  for (int tid = 0; tid < nr_workers; tid++) {
    gc_thds.push_back(std::thread([&, tid = tid]() {
      if (gc(stash_list) < 0)
        nr_failed++;
    }));
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

  auto end = chrono_utils::now();
  rmanager_->prof_reclaim_end(nr_workers, chrono_utils::duration(stt, end));
  return rmanager_->NumRegionAvail() >= 0;
}

int64_t Evacuator::force_reclaim() {
  if (!kEnableFaultHandler)
    return 0;

  auto stt = chrono_utils::now();
  rmanager_->prof_reclaim_stt();
  auto nr_workers = kNumEvacThds;

  int64_t nr_reclaimed = 0;
  std::vector<std::thread> thds;
  for (int i = 0; i < nr_workers; i++) {
    thds.emplace_back([&] {
      auto &segments = allocator_->segments_;
      while (rmanager_->NumRegionAvail() <= 0) {
        auto segment = segments.pop_front();
        if (!segment)
          break;
        if (segment.use_count() != 1) {
          MIDAS_LOG(kError) << segment << " " << segment.use_count();
        }
        assert(segment.use_count() <= 2);
        segment->destroy();
        nr_reclaimed++;
      }
    });
  }
  for (auto &thd : thds)
    thd.join();
  auto end = chrono_utils::now();
  rmanager_->prof_reclaim_end(nr_workers, chrono_utils::duration(stt, end));

  if (nr_reclaimed)
    MIDAS_LOG(kDebug) << "GC: " << nr_reclaimed << " force reclaimed ("
                      << chrono_utils::duration(stt, end) << "s). ";

  return nr_reclaimed;
}

/** util functions */
inline bool Evacuator::segment_ready(LogSegment *segment) {
  return segment->sealed() && !segment->destroyed();
}

/** Evacuate a particular segment */
inline RetCode Evacuator::iterate_segment(LogSegment *segment, uint64_t &pos,
                                          ObjectPtr &optr) {
  if (pos + sizeof(MetaObjectHdr) > segment->pos_)
    return ObjectPtr::RetCode::Fail;

  auto ret = optr.init_from_soft(TransientPtr(pos, sizeof(LargeObjectHdr)));
  if (ret == RetCode::Succ)
    pos += optr.obj_size(); // header size is already counted
  return ret;
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
  int nr_faulted = 0;
  int nr_contd_objs = 0;

  ObjectPtr obj_ptr;

  auto pos = segment->start_addr_;
  RetCode ret = RetCode::Fail;
  while ((ret = iterate_segment(segment, pos, obj_ptr)) == RetCode::Succ) {
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
            auto rref = reinterpret_cast<ObjectPtr *>(obj_ptr.get_rref());
            // assert(rref);
            // if (!rref)
            //   MIDAS_LOG(kError) << "null rref detected";
            auto ret = obj_ptr.free(/* locked = */ true);
            if (ret == RetCode::FaultLocal)
              goto faulted;
            // small objs are impossible to fault on other regions
            assert(ret != RetCode::FaultOther);
            if (rref && !rref->is_victim()) {
              auto vcache = pool_->get_vcache();
              vcache->put(rref, nullptr);
            }
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
              auto rref = reinterpret_cast<ObjectPtr *>(obj_ptr.get_rref());
              // assert(rref);
              // if (!rref)
              //   MIDAS_LOG(kError) << "null rref detected";
              // This will free all segments belonging to the same object
              auto ret = obj_ptr.free(/* locked = */ true);
              if (ret == RetCode::FaultLocal)
                goto faulted;
              // do nothing when ret == FaultOther and continue scanning
              if (rref && !rref->is_victim()) {
                auto vcache = pool_->get_vcache();
                vcache->put(rref, nullptr);
              }

              nr_freed++;
            }
          } else { // continued segment
            // An inner segment of a large object. Skip it.
            LargeObjectHdr lhdr;
            if (!load_hdr(lhdr, obj_ptr))
              goto faulted;
            auto head = lhdr.get_head();
            MetaObjectHdr head_hdr;
            if (head.null() || !load_hdr(head_hdr, head) ||
                !head_hdr.is_valid() || !head_hdr.is_present()) {
              nr_freed++;
            } else {
              alive_bytes += obj_size;
              nr_contd_objs++;
            }
          }
        } else
          nr_non_present++;
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_faulted++;
    obj_ptr.unlock(lock_id);
    break;
  }

  if (!kEnableFaultHandler)
    assert(nr_faulted == 0);
  if (deactivate) // meaning this is a scanning thread
    LogAllocator::count_alive(nr_present);
  MIDAS_LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
                    << ", nr_large_objs: " << nr_large_objs
                    << ", nr_non_present: " << nr_non_present
                    << ", nr_deactivated: " << nr_deactivated
                    << ", nr_freed: " << nr_freed
                    << ", nr_faulted: " << nr_faulted << ", alive ratio: "
                    << static_cast<float>(segment->alive_bytes_) /
                           kLogSegmentSize;

  assert(ret != RetCode::FaultOther);
  if (ret == RetCode::FaultLocal || nr_faulted) {
    if (!kEnableFaultHandler)
      MIDAS_LOG(kError) << "segment is unmapped under the hood";
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
  int nr_faulted = 0;
  int nr_contd_objs = 0;

  ObjectPtr obj_ptr;
  auto pos = segment->start_addr_;
  RetCode ret = RetCode::Fail;
  while ((ret = iterate_segment(segment, pos, obj_ptr)) == RetCode::Succ) {
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
          // MIDAS_LOG(kError) << "Failed to move the object!";
          nr_failed++;
        } else
          goto faulted;
      } else
        nr_failed++;
    } else {                         // large object
      if (!meta_hdr.is_continue()) { // the head segment of a large object.
        auto opt_data_size = obj_ptr.large_data_size();
        if (!opt_data_size) {
          // MIDAS_LOG(kWarning);
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
            // MIDAS_LOG(kError) << "Failed to move the object!";
            nr_failed++;
          } else if (ret == RetCode::FaultLocal) { // fault on src (obj_ptr)
            if (!kEnableFaultHandler)
              MIDAS_LOG(kWarning);
            goto faulted;
          }
          // skip when ret == FaultOther, meaning that fault is on dst (new_ptr)
        } else
          nr_failed++;
      } else { // an inner segment of a large obj
        LargeObjectHdr lhdr;
        if (!load_hdr(lhdr, obj_ptr))
          goto faulted;
        auto head = lhdr.get_head();
        MetaObjectHdr head_hdr;
        if (head.null() || !load_hdr(head_hdr, head) || !head_hdr.is_valid() ||
            !head_hdr.is_present()) {
          nr_freed++;
        } else {
          nr_contd_objs++;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_faulted++;
    obj_ptr.unlock(lock_id);
    break;
  }
  MIDAS_LOG(kDebug) << "nr_present: " << nr_present
                    << ", nr_moved: " << nr_moved << ", nr_freed: " << nr_freed
                    << ", nr_failed: " << nr_failed;

  if (!kEnableFaultHandler)
    assert(nr_faulted == 0);
  assert(ret != RetCode::FaultOther);
  if (ret == RetCode::FaultLocal || nr_faulted) {
    if (!kEnableFaultHandler)
      MIDAS_LOG(kError) << "segment is unmapped under the hood";
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
  int nr_faulted = 0;
  int nr_contd_objs = 0;

  ObjectPtr obj_ptr;
  auto pos = segment->start_addr_;
  RetCode ret = RetCode::Fail;
  while ((ret = iterate_segment(segment, pos, obj_ptr)) == RetCode::Succ) {
    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;

      MetaObjectHdr meta_hdr;
      if (!load_hdr(meta_hdr, obj_ptr))
        goto faulted;
      else {
        if (meta_hdr.is_present()) {
          auto ret = obj_ptr.free(/* locked = */ true);
          if (ret == RetCode::FaultLocal)
            goto faulted;
          assert(ret != RetCode::FaultOther);
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
            auto ret = obj_ptr.free(/* locked = */ true);
            if (ret == RetCode::FaultLocal)
              goto faulted;
            // skip the object and continue when ret == FaultOther
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
    nr_faulted++;
    obj_ptr.unlock(lock_id);
    break;
  }
  MIDAS_LOG(kDebug) << "nr_freed: " << nr_freed
                    << ", nr_non_present: " << nr_non_present
                    << ", nr_faulted: " << nr_faulted;

  if (!kEnableFaultHandler)
    assert(nr_faulted == 0);
  assert(ret != RetCode::FaultOther);
  if (ret == RetCode::FaultLocal || nr_faulted) {
    if (!kEnableFaultHandler)
      MIDAS_LOG(kError) << "segment is unmapped under the hood";
    segment->destroy();
    return EvacState::Fault;
  }
  if (nr_contd_objs)
    return EvacState::DelayRelease;
  segment->destroy();
  return EvacState::Succ;
}

} // namespace midas
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

#include "cache_manager.hpp"
#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

namespace midas {

using RetCode = ObjectPtr::RetCode;

/** LogSegment */
inline std::optional<ObjectPtr> LogSegment::alloc_small(size_t size) {
  if (sealed_ || destroyed_)
    return std::nullopt;
  auto obj_size = ObjectPtr::obj_size(size);
  if (pos_ - start_addr_ + obj_size > kLogSegmentSize) { // this segment is full
    seal();
    return std::nullopt;
  }
  ObjectPtr obj_ptr;
  if (obj_ptr.init_small(pos_, size) != RetCode::Succ) {
    if (!kEnableFaultHandler)
      MIDAS_LOG(kError);
    seal();
    return std::nullopt;
  }
  pos_ += obj_size;
  return obj_ptr;
}

inline std::optional<std::pair<TransientPtr, size_t>>
LogSegment::alloc_large(size_t size, const TransientPtr head_tptr,
                        TransientPtr prev_tptr) {
  if (sealed_ || destroyed_)
    return std::nullopt;
  if (pos_ - start_addr_ + sizeof(LargeObjectHdr) >= kLogSegmentSize) {
    seal();
    return std::nullopt;
  }

  ObjectPtr obj_ptr;
  size_t trunced_size = std::min(
      kLogSegmentSize - (pos_ - start_addr_) - sizeof(LargeObjectHdr), size);
  TransientPtr addr(pos_, sizeof(LargeObjectHdr) + trunced_size);
  const bool is_head = head_tptr.null();
  if (obj_ptr.init_large(pos_, trunced_size, is_head, head_tptr,
                         TransientPtr()) != RetCode::Succ)
    return std::nullopt;
  if (!prev_tptr.null()) {
    LargeObjectHdr lhdr;
    if (!load_hdr(lhdr, prev_tptr))
      return std::nullopt;
    lhdr.set_next(addr);
    if (!store_hdr(lhdr, prev_tptr))
      return std::nullopt;
  }

  pos_ += sizeof(LargeObjectHdr) + trunced_size;
  // if (pos_ - start_addr_ == kLogSegmentSize)
  //   seal();
  return std::make_pair(addr, trunced_size);
}

inline bool LogSegment::free(ObjectPtr &ptr) {
  return ptr.free() == RetCode::Succ;
}

void LogSegment::destroy() noexcept {
  destroyed_ = true;
  auto *rmanager = owner_->pool_->get_rmanager();
  rmanager->FreeRegion(region_id_);
  alive_bytes_ = kMaxAliveBytes;
}

/** LogAllocator */
// alloc a new segment
inline std::shared_ptr<LogSegment> LogAllocator::allocSegment(bool overcommit) {
  auto *rmanager = pool_->get_rmanager();
  int rid = rmanager->AllocRegion(overcommit);
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);

  return std::make_shared<LogSegment>(
      this, rid, reinterpret_cast<uint64_t>(range.stt_addr));
}

std::optional<ObjectPtr> LogAllocator::alloc_(size_t size, bool overcommit) {
  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (size >= kSmallObjThreshold) { // large obj
    return alloc_large(size, overcommit);
  }

  if (pcab_.local_seg && pcab_.local_seg->owner_ != this) {
    pcab_.local_seg->owner_->stashed_pcabs_.push_back(pcab_.local_seg);
    pcab_.local_seg.reset();
  }
  if (!pcab_.local_seg)
    pcab_.local_seg = stashed_pcabs_.pop_front();
  while (LIKELY(pcab_.local_seg.get() != nullptr)) {
    auto ret = pcab_.local_seg->alloc_small(size);
    if (ret)
      return ret;
    assert(pcab_.local_seg->sealed());
    // put pcab into segments_ and drop the reference so segments_ will be the
    // only owner.
    segments_.push_back(pcab_.local_seg);
    pcab_.local_seg = stashed_pcabs_.pop_front();
  }
  // slowpath
  auto segment = allocSegment(overcommit);
  if (!segment)
    return std::nullopt;

  // since pcab hold a reference to segment here, don't put segment into
  // segments_ for now. Instead, putting it back to segments_ when pcab is
  // sealed and guaranteed not be used anymore.
  pcab_.local_seg = segment;
  auto ret = pcab_.local_seg->alloc_small(size);
  assert(ret);
  return ret;
}

// Large objects
std::optional<ObjectPtr> LogAllocator::alloc_large(size_t size,
                                                   bool overcommit) {
  assert(size >= kSmallObjThreshold);

  ObjectPtr obj_ptr;
  int64_t remaining_size = size;
  TransientPtr head_tptr, prev_tptr;
  size_t alloced_size = 0;

  if (pcab_.local_seg && pcab_.local_seg->owner_ != this) {
    pcab_.local_seg->owner_->stashed_pcabs_.push_back(pcab_.local_seg);
    pcab_.local_seg.reset();
  }
  if (!pcab_.local_seg)
    pcab_.local_seg = stashed_pcabs_.pop_front();
  while (LIKELY(pcab_.local_seg.get() != nullptr)) {
    auto option = pcab_.local_seg->alloc_large(size, TransientPtr(), TransientPtr());
    if (option) {
      head_tptr = option->first;
      alloced_size = option->second;
      // do not seal pcab at this time as allocation may not finish. Instead,
      // seal pcab at the end of allocation.
      assert(alloced_size > 0);
      break;
    }
    // could be seg fault caused failure. The fault must happen
    // on this segment as there is no prev/next segment at
    // this point.
    if (!pcab_.local_seg->sealed())
      pcab_.local_seg->seal();
    segments_.push_back(pcab_.local_seg);

    pcab_.local_seg = stashed_pcabs_.pop_front();
  }
  remaining_size -= alloced_size;
  if (remaining_size <= 0) { // common path.
    assert(remaining_size == 0);
    if (obj_ptr.init_from_soft(head_tptr) != RetCode::Succ)
      return std::nullopt;
    return obj_ptr;
  }

  std::vector<std::shared_ptr<LogSegment>> alloced_segs;
  std::vector<TransientPtr> alloced_ptrs;
  if (head_tptr.null()) {
    auto segment = allocSegment(overcommit);
    if (!segment)
      goto failed;
    alloced_segs.push_back(segment);
    assert(remaining_size == size);
    auto option =
        segment->alloc_large(remaining_size, TransientPtr(), TransientPtr());
    if (segment->full())
      segment->seal();
    if (!option) // out of memory during allocation
      goto failed;
    head_tptr = option->first;
    alloced_size = option->second;
    remaining_size -= alloced_size;
  }
  if (head_tptr.null())
    goto failed;
  prev_tptr = head_tptr;
  while (remaining_size > 0) {
    alloced_ptrs.emplace_back(prev_tptr);
    auto segment = allocSegment(overcommit);
    if (!segment)
      goto failed;
    alloced_segs.push_back(segment);
    auto option = segment->alloc_large(remaining_size, head_tptr, prev_tptr);
    if (segment->full())
      segment->seal();
    if (!option) // out of memory during allocation
      goto failed;
    prev_tptr = option->first;
    alloced_size = option->second;
    remaining_size -= alloced_size;
  }
  if (obj_ptr.init_from_soft(head_tptr) != RetCode::Succ)
    goto failed;

  assert(!pcab_.local_seg || pcab_.local_seg->full());
  if (pcab_.local_seg && pcab_.local_seg->full()) {
    pcab_.local_seg->seal();
    segments_.push_back(pcab_.local_seg);
    pcab_.local_seg.reset();
  }
  assert(!pcab_.local_seg);
  if (!alloced_segs.empty()) {
    auto segment = alloced_segs.back();
    if (LIKELY(!segment->sealed())) {
      // If the last segment is not full, use it as pcab so skip putting it into
      // segments_ here.
      pcab_.local_seg = segment;
      alloced_segs.pop_back();
    }
  }
  for (auto &segment : alloced_segs)
    segments_.push_back(segment);
  return obj_ptr;

failed:
  if (!kEnableFaultHandler)
    MIDAS_LOG(kDebug) << "allocation failed!";
  for (auto &tptr : alloced_ptrs) {
    MetaObjectHdr mhdr;
    if (!load_hdr(mhdr, tptr))
      continue;
    mhdr.clr_present();
    if (!store_hdr(mhdr, tptr))
      continue;
  }
  for (auto &segment : alloced_segs)
    segment->destroy();
  return std::nullopt;
}

// Define PCAB
thread_local LogAllocator::PCAB LogAllocator::pcab_;
thread_local int32_t LogAllocator::access_cnt_ = 0;
thread_local int32_t LogAllocator::alive_cnt_ = 0;
std::atomic_int64_t LogAllocator::total_access_cnt_{0};
std::atomic_int64_t LogAllocator::total_alive_cnt_{0};

} // namespace midas
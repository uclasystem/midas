#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

namespace cachebank {

using RetCode = ObjectPtr::RetCode;

/** LogChunk */
inline std::optional<ObjectPtr> LogChunk::alloc_small(size_t size) {
  auto obj_size = ObjectPtr::obj_size(size);
  if (pos_ - start_addr_ + obj_size > kLogChunkSize) { // current chunk is full
    seal();
    return std::nullopt;
  }
  ObjectPtr obj_ptr;
  if (obj_ptr.init_small(pos_, size) != RetCode::Succ)
    return std::nullopt;
  pos_ += obj_size;
  return obj_ptr;
}

inline std::optional<std::pair<TransientPtr, size_t>>
LogChunk::alloc_large(size_t size, const TransientPtr head_tptr,
                      TransientPtr prev_tptr) {
  if (pos_ - start_addr_ + sizeof(LargeObjectHdr) >= kLogChunkSize) {
    seal();
    return std::nullopt;
  }

  ObjectPtr obj_ptr;
  size_t trunced_size = std::min(
      kLogChunkSize - (pos_ - start_addr_) - sizeof(LargeObjectHdr), size);
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
  // if (pos_ - start_addr_ == kLogChunkSize)
  //   seal();
  return std::make_pair(addr, trunced_size);
}

inline bool LogChunk::free(ObjectPtr &ptr) {
  return ptr.free() == RetCode::Succ;
}

/** LogSegment */
inline std::shared_ptr<LogChunk> LogSegment::allocChunk() {
  if (full()) {
    seal();
    return nullptr;
  }

  uint64_t addr = pos_;
  pos_ += kLogChunkSize;
  if (pos_ >= start_addr_ + kLogChunkSize)
    seal();
  auto chunk = std::make_shared<LogChunk>(this, addr);
  vLogChunks_.push_back(chunk);
  return chunk;
}

void LogSegment::destroy() {
  while (!vLogChunks_.empty()) {
    vLogChunks_.pop_back();
  }

  auto *rmanager = ResourceManager::global_manager();
  rmanager->FreeRegion(region_id_);
  alive_bytes_ = kMaxAliveBytes;
  destroyed_ = true;
}

/** LogAllocator */
// alloc a new segment
inline std::shared_ptr<LogSegment> LogAllocator::allocSegment(bool overcommit) {
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion(overcommit);
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);

  auto segment = std::make_shared<LogSegment>(
      rid, reinterpret_cast<uint64_t>(range.stt_addr));

  return segment;
}

inline std::shared_ptr<LogChunk> LogAllocator::allocChunk(bool overcommit) {
  auto segment = allocSegment(overcommit);
  if (!segment)
    return nullptr;

  segments_.push_back(segment);
  return segment->allocChunk();
}

std::optional<ObjectPtr> LogAllocator::alloc_(size_t size, bool overcommit) {
  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (size >= kSmallObjThreshold) { // large obj
    return alloc_large(size, overcommit);
  }

  if (LIKELY(pcab.get() != nullptr)) {
    auto ret = pcab->alloc_small(size);
    if (ret)
      return ret;
    pcab.reset();
  }
  // slowpath
  auto chunk = allocChunk(overcommit);
  if (!chunk)
    return std::nullopt;

  pcab = chunk;
  auto ret = pcab->alloc_small(size);
  assert(ret);
  return ret;
}

// Large objects
std::optional<ObjectPtr> LogAllocator::alloc_large(size_t size,
                                                   bool overcommit) {
  assert(size >= kSmallObjThreshold);

  ObjectPtr obj_ptr;
  int64_t remaining_size = size;

  if (UNLIKELY(pcab.get() == nullptr)) {
    auto chunk = allocChunk(overcommit);
    if (!chunk)
      return std::nullopt;
    pcab = chunk;
  }
  auto option = pcab->alloc_large(size, TransientPtr(), TransientPtr());
  if (!option) { // Rare path: the chunk is already full.
    pcab.reset();
    auto chunk = allocChunk(overcommit);
    if (!chunk)
      return std::nullopt;
    pcab = chunk;
    option = pcab->alloc_large(size, TransientPtr(), TransientPtr());
  }
  assert(option);
  auto [head_tptr, alloced_size] = *option;
  remaining_size -= alloced_size;
  if (remaining_size <= 0) { // common path.
    assert(remaining_size == 0);
    if (obj_ptr.init_from_soft(head_tptr) != RetCode::Succ)
      return std::nullopt;
    return obj_ptr;
  }

  std::vector<TransientPtr> alloced_ptrs;
  auto prev_tptr = head_tptr;
  while (remaining_size > 0) {
    alloced_ptrs.emplace_back(prev_tptr);
    auto option = pcab->alloc_large(remaining_size, head_tptr, prev_tptr);
    if (!option) {
      pcab.reset();
      auto chunk = allocChunk(overcommit);
      if (!chunk)
        goto failed;
      pcab = chunk;
      option = pcab->alloc_large(remaining_size, head_tptr, prev_tptr);
    }
    if (!option) // out of memory during allocation
      goto failed;
    prev_tptr = option->first;
    alloced_size = option->second;
    remaining_size -= alloced_size;
  }
  if (obj_ptr.init_from_soft(head_tptr) != RetCode::Succ)
    goto failed;

  return obj_ptr;
failed:
  for (auto &tptr : alloced_ptrs) {
    MetaObjectHdr mhdr;
    if (!tptr.copy_to(&mhdr, sizeof(mhdr)))
      continue;
    mhdr.clr_present();
    if (!tptr.copy_from(&mhdr, sizeof(mhdr)))
      continue;
  }
  return std::nullopt;
}

// Define PCAB
thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;
thread_local int32_t LogAllocator::access_cnt_ = 0;
thread_local int32_t LogAllocator::alive_cnt_ = 0;
std::atomic_int64_t LogAllocator::total_access_cnt_{0};
std::atomic_int64_t LogAllocator::total_alive_cnt_{0};

} // namespace cachebank
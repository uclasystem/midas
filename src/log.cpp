#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>

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
  auto obj_size = ObjectPtr::total_size(size);
  if (pos_ + obj_size + sizeof(MetaObjectHdr) >=
      start_addr_ + kLogChunkSize) { // current chunk is full
    assert(!full());
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
LogChunk::alloc_large(size_t size, TransientPtr head_tptr,
                      TransientPtr prev_tptr) {
  if (pos_ + sizeof(LargeObjectHdr) >= start_addr_ + kLogChunkSize) {
    LOG(kError) << "Chunk is full during large allocation!";
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
    if (!prev_tptr.copy_to(&lhdr, sizeof(lhdr)))
      return std::nullopt;
    lhdr.set_next(addr);
    if (!prev_tptr.copy_from(&lhdr, sizeof(lhdr)))
      return std::nullopt;
  }

  pos_ += sizeof(LargeObjectHdr) + trunced_size;
  seal();

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
// must be called under lock protection
// try to get a non-empty chunk
inline std::shared_ptr<LogChunk> LogAllocator::getChunk() {
  std::unique_lock<std::mutex> ul(lock_);
  auto segment = getSegment();
  if (!segment)
    return nullptr;
  return segment->allocChunk();
}

// try to get a non-empty segment
inline std::shared_ptr<LogSegment> LogAllocator::getSegment() {
  if (!vSegments_.empty()) {
    auto segment = vSegments_.back();
    if (!segment->full())
      return segment;
    segment->seal();
  }
  return nullptr;
}

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
  std::shared_ptr<LogChunk> chunk = getChunk();

  auto segment = allocSegment(overcommit);
  if (!segment)
    return nullptr;

  std::unique_lock<std::mutex> ul(lock_);
  vSegments_.push_back(segment);
  return segment->allocChunk();
}

std::optional<ObjectPtr> LogAllocator::alloc_(size_t size, bool overcommit) {
  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (size >= kSmallObjThreshold) { // large obj
    // LOG(kError) << "large obj allocation is not implemented yet!";
    return alloc_large(size);
  }

  if (pcab.get()) {
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
std::optional<ObjectPtr> LogAllocator::alloc_large(size_t size) {
  assert(size >= kSmallObjThreshold);

  ObjectPtr obj_ptr;

  std::vector<std::shared_ptr<LogSegment>> segments;
  std::vector<std::shared_ptr<LogChunk>> chunks;
  {
    int64_t remaining_size = size;

    auto segment = allocSegment();
    if (!segment)
      goto failed;
    segments.push_back(segment);
    auto head_chunk = segment->allocChunk();
    assert(head_chunk.get());
    chunks.push_back(head_chunk);

    auto option =
        head_chunk->alloc_large(remaining_size, TransientPtr(), TransientPtr());
    if (!option)
      goto failed;

    auto [head_tptr, alloced_size] = *option;
    if (obj_ptr.init_from_soft(head_tptr) != RetCode::Succ)
      goto failed;

    auto prev_tptr = head_tptr;
    remaining_size -= alloced_size;
    while (remaining_size > 0) {
      auto chunk = segment->allocChunk();
      if (!chunk) {
        segment = allocSegment();
        if (!segment)
          goto failed;
        chunk = segment->allocChunk();
        if (!chunk)
          goto failed;
      }
      chunks.push_back(chunk);

      auto option = chunk->alloc_large(remaining_size, head_tptr, prev_tptr);
      if (!option)
        goto failed;
      prev_tptr = option->first;
      alloced_size = option->second;
      remaining_size -= alloced_size;
    }
  }

  lock_.lock();
  for (auto segment : segments)
    vSegments_.push_back(segment);
  lock_.unlock();
  return obj_ptr;

failed:
  for (auto segment : segments)
    segment->destroy();

  return std::nullopt;
}

// Define PCAB
thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;
thread_local int32_t LogAllocator::access_cnt_ = 0;
thread_local int32_t LogAllocator::alive_cnt_ = 0;
std::atomic_int64_t LogAllocator::total_access_cnt_{0};
std::atomic_int64_t LogAllocator::total_alive_cnt_{0};

} // namespace cachebank
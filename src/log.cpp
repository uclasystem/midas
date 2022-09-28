#include <cstddef>
#include <cstdint>
#include <mutex>

#include "log.hpp"

namespace cachebank {

inline TransientPtr LogChunk::alloc(size_t size) {
  if (pos_ + kObjHdrSize + size > start_addr_ + kLogChunkSize)
    goto failed;
  else {
    auto objHdr = ObjectHdr();
    objHdr.set(size);
    auto hdrPtr =
        TransientPtr(reinterpret_cast<ObjectHdr *>(pos_), sizeof(ObjectHdr));
    if (!hdrPtr.copy_from(&objHdr, sizeof(ObjectHdr)))
      goto failed;
    void *addr = reinterpret_cast<void *>(pos_ + kObjHdrSize);
    pos_ += kObjHdrSize + size;
    return TransientPtr(addr, size);
  }

failed:
  return TransientPtr();
}

inline bool LogChunk::free(size_t addr) {
  auto hdrPtr =
      TransientPtr(reinterpret_cast<ObjectHdr *>(addr - sizeof(ObjectHdr)),
                   sizeof(ObjectHdr));
  uint32_t flags = ObjectHdr::kInvalidFlags;
  if (!hdrPtr.copy_from(&flags, sizeof(flags), offsetof(ObjectHdr, flags)))
    goto failed;

failed:
  return false;
}

uint64_t LogRegion::allocChunk() {
  if (full())
    return -ENOMEM;

  uint64_t addr = pos_;
  pos_ += kPageChunkSize;
  return addr;
}

bool LogAllocator::allocRegion() {
  std::unique_lock<std::mutex> ul(lock_);
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion();
  if (rid == -1)
    goto failed;
  else {
    VRange range = rmanager->GetRegion(rid);
    vRegions_.push_back(std::make_shared<LogRegion>(
        reinterpret_cast<uint64_t>(range.stt_addr)));
  }

  return true;
failed:
  return false;
}

bool LogAllocator::allocChunk() {
retry:
  std::unique_lock<std::mutex> ul(lock_);
  if (curr_region_ >= vRegions_.size()) {
    ul.unlock();
    allocRegion();
    goto retry;
  }
  uint64_t chunk_addr = vRegions_[curr_region_]->allocChunk();
  if (chunk_addr == -ENOMEM) {
    curr_region_++;
    ul.unlock();
    goto retry;
  }
  vLogChunks_.push_back(std::make_shared<LogChunk>(chunk_addr));
  return true;

failed:
  return false;
}

TransientPtr LogAllocator::alloc(size_t size) {
retry:
  if (curr_chunk_ >= vLogChunks_.size()) {
    allocChunk();
    goto retry;
  }
  auto chunk = vLogChunks_[curr_chunk_];
  auto tptr = chunk->alloc(size);
  if (!tptr.is_valid()) {
    curr_chunk_++;
    goto retry;
  }
  return tptr;
}

bool LogAllocator::free(TransientPtr &ptr) {
  if (!ptr.is_valid())
    goto failed;
  else {
    TransientPtr hdrPtr = ptr.slice(-sizeof(ObjectHdr), sizeof(ObjectHdr));
    uint32_t flags = 1 << ObjectHdr::kInvalidFlags;
    if (!hdrPtr.copy_from(&flags, sizeof(flags), offsetof(ObjectHdr, flags)))
      goto failed;
    return true;
  }
failed:
  return false;
}

} // namespace cachebank
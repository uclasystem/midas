#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "log.hpp"

namespace cachebank {

inline std::optional<TransientPtr> LogChunk::alloc(size_t size) {
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
  return std::nullopt;
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

// must be called under lock protection
bool LogAllocator::allocRegion() {
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

inline bool LogAllocator::_allocChunk() {
  if (vRegions_.empty())
    return false;
  uint64_t chunk_addr = vRegions_.back().get()->allocChunk();
  if (chunk_addr == -ENOMEM)
    return false;
  vLogChunks_.push_back(std::make_shared<LogChunk>(chunk_addr));
  return true;
}

bool LogAllocator::allocChunk() {
  std::unique_lock<std::mutex> ul(lock_);
  if (_allocChunk())
    return true;
  if (!allocRegion() || !_allocChunk())
    return false;
  pcab = vLogChunks_.back();
  return true;
}

std::optional<TransientPtr> LogAllocator::alloc(size_t size) {
  if (pcab.get()) {
    auto ret = pcab->alloc(size);
    if (ret)
      return ret;
    else
      pcab->seal();
  }

  if (!allocChunk())
    return std::nullopt;

  auto ret = pcab->alloc(size);
  assert(ret);
  return ret;
}

bool LogAllocator::free(TransientPtr &ptr) {
  if (!ptr.is_valid())
    goto failed;
  else {
    // get object header with offse
    TransientPtr hdrPtr = ptr.slice(-sizeof(ObjectHdr), sizeof(ObjectHdr));
    uint32_t flags = 1 << ObjectHdr::kInvalidFlags;
    if (!hdrPtr.copy_from(&flags, sizeof(flags), offsetof(ObjectHdr, flags)))
      goto failed;
    return true;
  }
failed:
  return false;
}

thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
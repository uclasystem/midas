#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "log.hpp"
#include "object.hpp"

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
  ObjectHdr hdr;
  if (!hdrPtr.copy_to(&hdr, sizeof(ObjectHdr)))
    goto failed;
  hdr.clr_present();
  if (!hdrPtr.copy_from(&hdr, sizeof(ObjectHdr)))
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
std::shared_ptr<LogRegion> LogAllocator::getRegion() {
  if (!vRegions_.empty()) {
    auto region = vRegions_.back();
    if (!region->full())
      return region;
  }

  // alloc a new region
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion();
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);

  auto region =
      std::make_shared<LogRegion>(reinterpret_cast<uint64_t>(range.stt_addr));
  vRegions_.push_back(region);

  return region;
}

std::shared_ptr<LogChunk> LogAllocator::allocChunk() {
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t chunk_addr = -ENOMEM;
  auto region = getRegion();
  if (region) {
    chunk_addr = region->allocChunk();
    if (chunk_addr == -ENOMEM)
      region->seal();
    else
      goto done;
  }

  region = getRegion();
  if (!region)
    return nullptr;

  chunk_addr = region->allocChunk();
  if (chunk_addr == -ENOMEM)
    return nullptr;

done:
  auto chunk = std::make_shared<LogChunk>(chunk_addr);
  vLogChunks_.push_back(chunk);
  return chunk;
}

std::optional<TransientPtr> LogAllocator::alloc(size_t size) {
  if (pcab.get()) {
    auto ret = pcab->alloc(size);
    if (ret)
      return ret;
    // current pcab is full
    pcab->seal();
  }
  // slowpath
  auto chunk = allocChunk();
  if (!chunk)
    return std::nullopt;

  pcab = chunk;
  auto ret = pcab->alloc(size);
  assert(ret);
  return ret;
}

bool LogAllocator::free(TransientPtr &ptr) {
  if (!ptr.is_valid())
    return false;
  // get object header with offset
  TransientPtr hdrPtr = ptr.slice(-sizeof(ObjectHdr), sizeof(ObjectHdr));
  // uint32_t flags = 1 << ObjectHdr::kInvalidFlags;
  ObjectHdr hdr;
  if (!hdrPtr.copy_to(&hdr, sizeof(ObjectHdr)))
    return false;
  hdr.clr_present();
  if (!hdrPtr.copy_from(&hdr, sizeof(ObjectHdr)))
    return false;
  return true;
}

thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
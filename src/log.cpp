#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "log.hpp"
#include "resource_manager.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "utils.hpp"

namespace cachebank {

inline std::optional<TransientPtr> LogChunk::alloc(size_t size) {
  auto hdr_size = sizeof(SmallObjectHdr);
  if (pos_ + hdr_size + size >
      start_addr_ + kLogChunkSize - sizeof(GenericObjectHdr))
    goto full;
  else {
    SmallObjectHdr objHdr;
    objHdr.init(size);
    auto hdrPtr =
        TransientPtr(reinterpret_cast<SmallObjectHdr *>(pos_), hdr_size);
    if (!hdrPtr.copy_from(&objHdr, hdr_size))
      goto failed;
    void *addr = reinterpret_cast<void *>(pos_ + hdr_size);
    pos_ += hdr_size + size;
    return TransientPtr(addr, size);
  }

full: // current chunk is full
  seal();
failed:
  return std::nullopt;
}

inline bool LogChunk::free(size_t addr) {
  auto hdrPtr = TransientPtr(
      reinterpret_cast<SmallObjectHdr *>(addr - sizeof(SmallObjectHdr)),
      sizeof(SmallObjectHdr));
  SmallObjectHdr hdr;
  if (!hdrPtr.copy_to(&hdr, sizeof(SmallObjectHdr)))
    goto failed;
  hdr.free();
  if (!hdrPtr.copy_from(&hdr, sizeof(SmallObjectHdr)))
    goto failed;

failed:
  return false;
}

uint64_t LogRegion::allocChunk() {
  if (full()) {
    seal();
    return -ENOMEM;
  }

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
    if (chunk_addr != -ENOMEM)
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
  if (size >= kSmallObjThreshold) { // large obj
    LOG(kError) << "large obj allocation is not implemented yet!";
    return std::nullopt;
  }

  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (pcab.get()) {
    auto ret = pcab->alloc(size);
    if (ret)
      return ret;
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

  if (ptr.size() >= kSmallObjThreshold) { // large obj
    LOG(kError) << "large obj allocation is not implemented yet!";
    return false;
  }
  // get object header with offset
  TransientPtr hdrPtr =
      ptr.slice(-sizeof(SmallObjectHdr), sizeof(SmallObjectHdr));
  SmallObjectHdr objHdr;
  if (!hdrPtr.copy_to(&objHdr, sizeof(SmallObjectHdr)))
    return false;
  objHdr.free();
  if (!hdrPtr.copy_from(&objHdr, sizeof(SmallObjectHdr)))
    return false;
  return true;
}

thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
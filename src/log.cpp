#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

namespace cachebank {

/** LogChunk */
inline std::optional<TransientPtr> LogChunk::alloc(size_t size) {
  auto hdr_size = sizeof(SmallObjectHdr);
  if (pos_ + hdr_size + size + sizeof(GenericObjectHdr) >=
      start_addr_ + kLogChunkSize)
    goto full;
  else {
    SmallObjectHdr objHdr;
    objHdr.init(size);
    auto hdrPtr = TransientPtr(pos_, hdr_size);
    if (!hdrPtr.copy_from(&objHdr, hdr_size))
      goto failed;
    auto next_pos = pos_ + hdr_size;
    pos_ += hdr_size + size;
    return TransientPtr(next_pos, size);
  }

full: // current chunk is full
  seal();
failed:
  return std::nullopt;
}

inline bool LogChunk::free(size_t addr) {
  auto hdrPtr =
      TransientPtr(addr - sizeof(SmallObjectHdr), sizeof(SmallObjectHdr));
  SmallObjectHdr hdr;
  if (!hdrPtr.copy_to(&hdr, sizeof(SmallObjectHdr)))
    goto failed;
  hdr.free();
  if (!hdrPtr.copy_from(&hdr, sizeof(SmallObjectHdr)))
    goto failed;

failed:
  return false;
}

void LogChunk::scan() {
  int nr_deactivated = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;

  GenericObjectHdr hdr;

  auto pos = start_addr_;
  auto tptr = TransientPtr(pos, sizeof(GenericObjectHdr));
  bool chunk_unmapped = false;
  while (pos < pos_) {
    if (!tptr.copy_to(&hdr, sizeof(hdr))) {
      LOG(kError) << "chunk is unmapped under the hood";
      chunk_unmapped = true;
      break;
    }
    if (!hdr.is_valid()) { // encounter the sentinel pointer, finish this chunk.
      break;
    }
    if (hdr.is_small_obj()) {
      SmallObjectHdr shdr;
      shdr = *(reinterpret_cast<SmallObjectHdr *>(&hdr));
      // LOG(kInfo) << shdr.get_size() << " " << shdr.is_present() << " "
      //            << reinterpret_cast<void *>(shdr.get_rref());

      if (shdr.is_present()) {
        if (shdr.is_accessed()) {
          shdr.clr_accessed();
          nr_deactivated++;
        } else {
          shdr.clr_present();
          nr_freed++;
        }
      }
      nr_small_objs++;
      if (!tptr.copy_from(&shdr, sizeof(SmallObjectHdr))) {
        LOG(kError) << "chunk is unmapped under the hood";
        chunk_unmapped = true;
        break;
      }

      auto obj_size = shdr.get_size();
      pos += sizeof(SmallObjectHdr) + obj_size;
      tptr = TransientPtr(pos, sizeof(GenericObjectHdr));
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      LargeObjectHdr lhdr;
      auto obj_size = lhdr.get_size();
      if (!tptr.copy_to(&lhdr, sizeof(lhdr))) {
        LOG(kError) << "chunk is unmapped under the hood";
        chunk_unmapped = true;
        break;
      }
      if (lhdr.is_continue()) {
        // this is a inner chunk storing a large object.
      } else {
        // this is the head chunk of a large object.
      }
    }
  }
  LOG(kInfo) << "nr_scanned_small_objs: " << nr_small_objs
             << ", nr_deactivated: " << nr_deactivated
             << ", nr_freed: " << nr_freed;
}

void LogChunk::evacuate() {
  if (!sealed_)
    return;

  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;

  GenericObjectHdr hdr;

  auto pos = start_addr_;
  auto tptr = TransientPtr(pos, sizeof(GenericObjectHdr));
  bool chunk_unmapped = false;
  while (pos < pos_) {
    if (!tptr.copy_to(&hdr, sizeof(hdr))) {
      LOG(kError) << "chunk is unmapped under the hood";
      chunk_unmapped = true;
      break;
    }
    if (!hdr.is_valid()) { // encounter the sentinel pointer, finish this chunk.
      break;
    }
    if (hdr.is_small_obj()) {
      SmallObjectHdr shdr;
      shdr = *(reinterpret_cast<SmallObjectHdr *>(&hdr));
      auto obj_size = shdr.get_size();
      if (shdr.is_present()) {
        nr_present++;
        auto allocator = LogAllocator::global_allocator();
        auto optptr = allocator->alloc(obj_size);
        if (optptr) {
          auto new_ptr = *optptr;
          if (new_ptr.copy_from(tptr.slice(sizeof(GenericObjectHdr), obj_size),
                                obj_size)) {
            nr_moved++;
          }
        }
        shdr.free();
      } else {
        nr_freed++;
      }
      nr_small_objs++;

      if (!tptr.copy_from(&shdr, sizeof(SmallObjectHdr))) {
        LOG(kError) << "chunk is unmapped under the hood";
        chunk_unmapped = true;
        break;
      }

      pos += sizeof(SmallObjectHdr) + obj_size;
      tptr = TransientPtr(pos, sizeof(GenericObjectHdr));
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      LargeObjectHdr lhdr;
      auto obj_size = lhdr.get_size();
      if (!tptr.copy_to(&lhdr, sizeof(lhdr))) {
        LOG(kError) << "chunk is unmapped under the hood";
        chunk_unmapped = true;
        break;
      }
      if (lhdr.is_continue()) {
        // this is a inner chunk storing a large object.
      } else {
        // this is the head chunk of a large object.
      }
    }
  }
  LOG(kInfo) << "nr_present: " << nr_present << ", nr_moved: " << nr_moved
             << ", nr_freed: " << nr_freed;
}

/** LogRegion */
inline std::shared_ptr<LogChunk> LogRegion::allocChunk() {
  if (full()) {
    seal();
    return nullptr;
  }

  uint64_t addr = pos_;
  pos_ += kLogChunkSize;
  auto chunk = std::make_shared<LogChunk>(addr);
  vLogChunks_.push_back(chunk);
  return chunk;
}

void LogRegion::destroy() {
  while (!vLogChunks_.empty()) {
    vLogChunks_.pop_back();
  }

  auto *rmanager = ResourceManager::global_manager();
  rmanager->FreeRegion(region_id_);
  destroyed_ = true;
}

void LogRegion::scan() {
  for (auto &chunk : vLogChunks_) {
    chunk->scan();
  }
}

void LogRegion::evacuate() {
  if (!sealed_)
    return;
  for (auto &chunk : vLogChunks_) {
    chunk->evacuate();
  }
  destroy();
}

/** LogAllocator */
// must be called under lock protection
inline std::shared_ptr<LogRegion> LogAllocator::getRegion() {
  if (!vRegions_.empty()) {
    auto region = vRegions_.back();
    if (region->full())
      region->seal();
    else
      return region;
  }

  // alloc a new region
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion();
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);

  auto region = std::make_shared<LogRegion>(
      rid, reinterpret_cast<uint64_t>(range.stt_addr));
  vRegions_.push_back(region);

  return region;
}

inline std::shared_ptr<LogChunk> LogAllocator::allocChunk() {
  std::unique_lock<std::mutex> ul(lock_);
  std::shared_ptr<LogChunk> chunk = nullptr;
  auto region = getRegion();
  if (region) {
    chunk = region->allocChunk();
    if (chunk != nullptr)
      return chunk;
  }

  region = getRegion();
  if (!region)
    return nullptr;
  return region->allocChunk();
}

std::optional<TransientPtr> LogAllocator::alloc(size_t size) {
  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (size >= kSmallObjThreshold) { // large obj
    LOG(kError) << "large obj allocation is not implemented yet!";
    return std::nullopt;
  }

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
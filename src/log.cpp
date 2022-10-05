#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

#include "log.hpp"
#include "logging.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "utils.hpp"

namespace cachebank {

/** LogChunk */
inline std::optional<ObjectPtr> LogChunk::alloc(size_t size) {
  auto obj_size = ObjectPtr::total_size(size);
  if (pos_ + obj_size + sizeof(GenericObjectHdr) >=
      start_addr_ + kLogChunkSize)
    goto full;
  else {
    ObjectPtr obj_ptr;
    if (!obj_ptr.set(pos_, size))
      goto failed;
    pos_ += obj_size;
    return obj_ptr;
  }

full: // current chunk is full
  seal();
failed:
  return std::nullopt;
}

inline bool LogChunk::free(ObjectPtr &ptr) {
  return ptr.free();
}

void LogChunk::scan() {
  int nr_deactivated = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;

  GenericObjectHdr hdr;

  auto pos = start_addr_;
  bool chunk_unmapped = false;
  while (pos < pos_) {
    ObjectPtr obj_ptr;
    if (!obj_ptr.init_from_soft(pos)) {
      LOG(kError) << "chunk is unmapped under the hood";
      chunk_unmapped = true;
      break;
    }
    if (!obj_ptr.is_valid()) { // encounter the sentinel pointer, finish this chunk.
      break;
    }
    if (obj_ptr.is_small_obj()) {
      // LOG(kInfo) << shdr.get_size() << " " << shdr.is_present() << " "
      //            << reinterpret_cast<void *>(shdr.get_rref());

      nr_small_objs++;

      bool ret = true;
      if (obj_ptr.is_present()) {
        if (obj_ptr.is_accessed()) {
          ret &= obj_ptr.clr_accessed();
          nr_deactivated++;
        } else {
          ret &= obj_ptr.clr_present();
          nr_freed++;
        }
      }
      if (!ret) {
        LOG(kError) << "chunk is unmapped under the hood";
        chunk_unmapped = true;
        break;
      }

      pos += obj_ptr.total_size();
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      if (obj_ptr.is_continue()) {
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

  auto pos = start_addr_;
  bool chunk_unmapped = false;
  while (pos < pos_) {
    ObjectPtr obj_ptr;
    if (!obj_ptr.init_from_soft(pos)) {
      LOG(kError) << "chunk is unmapped under the hood";
      chunk_unmapped = true;
      break;
    }
    if (!obj_ptr.is_valid()) { // encounter the sentinel pointer, finish this chunk.
      break;
    }
    if (obj_ptr.is_small_obj()) {
      if (obj_ptr.is_present()) {
        nr_present++;
        auto allocator = LogAllocator::global_allocator();
        auto optptr = allocator->alloc(obj_ptr.data_size());
        if (optptr) {
          auto new_ptr = *optptr;
          if (new_ptr.copy_from(obj_ptr, obj_ptr.data_size())) {
            nr_moved++;
          }
        }
        if (!obj_ptr.free()) {
          chunk_unmapped = true;
          break;
        }
      } else {
        nr_freed++;
      }
      nr_small_objs++;

      pos += obj_ptr.total_size();
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      if (obj_ptr.is_continue()) {
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

std::optional<ObjectPtr> LogAllocator::alloc(size_t size) {
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

bool LogAllocator::free(ObjectPtr &ptr) {
  return ptr.free();
}

thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
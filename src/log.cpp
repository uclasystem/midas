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

using RetCode = ObjectPtr::RetCode;

/** LogChunk */
inline std::optional<ObjectPtr> LogChunk::alloc(size_t size) {
  auto obj_size = ObjectPtr::total_size(size);
  if (pos_ + obj_size + sizeof(GenericObjectHdr) >=
      start_addr_ + kLogChunkSize) { // current chunk is full
    assert(!full());
    seal();
    return std::nullopt;
  }
  ObjectPtr obj_ptr;
  if (obj_ptr.set(pos_, size) != RetCode::Succ)
    return std::nullopt;
  pos_ += obj_size;
  return obj_ptr;
}

inline bool LogChunk::free(ObjectPtr &ptr) {
  return ptr.free() == RetCode::Succ;
}

bool LogChunk::scan() {
  if (!sealed_)
    return false;
  int nr_deactivated = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;

  GenericObjectHdr hdr;

  auto pos = start_addr_;
  while (pos < pos_) {
    ObjectPtr obj_ptr;

    auto ret = obj_ptr.init_from_soft(pos);
    if (ret == RetCode::Fail) { // the sentinel pointer, done this chunk.
      break;
    } else if (ret == RetCode::Fault) {
      LOG(kError) << "chunk is unmapped under the hood";
      break;
    }
    assert(ret == RetCode::Succ);

    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      auto obj_size = obj_ptr.total_size();
      nr_small_objs++;

      RetCode ret = obj_ptr.is_present();
      if (ret == RetCode::True) {
        ret = obj_ptr.is_accessed();
        if (ret == RetCode::True) {
          if (obj_ptr.clr_accessed() == RetCode::Fault)
            goto faulted;
          nr_deactivated++;
        } else {
          if (obj_ptr.free(/* locked = */true) == RetCode::Fault)
            goto faulted;
          nr_freed++;
        }
      } else if (ret == RetCode::Fault)
        goto faulted;

      pos += obj_size;
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      ret = obj_ptr.is_continue();
      if (ret == RetCode::Fault)
        goto faulted;
      if (ret == RetCode::True) {
        // this is a inner chunk storing a large object.
      } else {
        // this is the head chunk of a large object.
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kInfo) << "nr_scanned_small_objs: " << nr_small_objs
             << ", nr_deactivated: " << nr_deactivated
             << ", nr_freed: " << nr_freed;
  return true;
}

bool LogChunk::evacuate() {
  if (!sealed_)
    return false;

  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;

  auto pos = start_addr_;
  while (pos < pos_) {
    ObjectPtr obj_ptr;
    auto ret = obj_ptr.init_from_soft(pos);
    if (ret == RetCode::Fail) { // the sentinel pointer, done this chunk.
      break;
    } else if (ret == RetCode::Fault) {
      LOG(kError) << "chunk is unmapped under the hood";
      break;
    }
    assert(ret == RetCode::Succ);

    auto lock_id = obj_ptr.lock();
    assert(lock_id != -1 && !obj_ptr.null());
    if (obj_ptr.is_small_obj()) {
      nr_small_objs++;
      auto obj_size = obj_ptr.total_size();
      ret = obj_ptr.is_present();
      if (ret == RetCode::True) {
        nr_present++;
        auto allocator = LogAllocator::global_allocator();
        auto optptr = allocator->alloc(obj_ptr.data_size());
        if (optptr) {
          auto new_ptr = *optptr;
          ret = new_ptr.move_from(obj_ptr);
          if (ret == RetCode::Succ) {
            nr_moved++;
          } else if (ret == RetCode::Fail) {
            LOG(kError) << "Failed to move the object!";
          } else
            goto faulted;
        }
      } else {
        if (ret == RetCode::Fault)
          goto faulted;
        nr_freed++;
      }

      pos += obj_size;
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      ret = obj_ptr.is_continue();
      if (ret == RetCode::True) {
        // this is a inner chunk storing a large object.
      } else if (ret == RetCode::False) {
        // this is the head chunk of a large object.
      } else
        goto faulted;
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kInfo) << "nr_present: " << nr_present << ", nr_moved: " << nr_moved
             << ", nr_freed: " << nr_freed;
  return true;
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
  bool ret = true;
  for (auto &chunk : vLogChunks_) {
    ret &= chunk->evacuate();
  }
  if (ret)
    destroy();
}

/** LogAllocator */
// must be called under lock protection
inline std::shared_ptr<LogRegion> LogAllocator::getRegion() {
  if (!vRegions_.empty()) {
    auto region = vRegions_.back();
    if (!region->full())
      return region;
    region->seal();
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

thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
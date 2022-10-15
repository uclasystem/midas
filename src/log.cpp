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
#include "utils.hpp"

namespace cachebank {

using RetCode = ObjectPtr::RetCode;

/** LogChunk */
inline std::optional<ObjectPtr> LogChunk::alloc(size_t size) {
  auto obj_size = ObjectPtr::total_size(size);
  if (pos_ + obj_size + sizeof(MetaObjectHdr) >=
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
  alive_bytes_ = 0;

  int nr_deactivated = 0;
  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

  MetaObjectHdr hdr;

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

      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_present()) {
          if (meta_hdr.is_accessed()) {
            meta_hdr.clr_accessed();
            if (!store_hdr<>(meta_hdr, obj_ptr))
              goto faulted;
            nr_deactivated++;
            upd_alive_bytes(obj_size);
          } else {
            if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
              goto faulted;
            nr_freed++;
          }
        } else if (ret == RetCode::False)
          nr_non_present++;
      }

      pos += obj_size;
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_scanned_small_objs: " << nr_small_objs
              << ", nr_non_present: " << nr_non_present
              << ", nr_deactivated: " << nr_deactivated
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed
              << ", alive ratio: "
              << static_cast<float>(alive_bytes_) / kLogChunkSize;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

bool LogChunk::evacuate() {
  if (!sealed_)
    return false;

  int nr_present = 0;
  int nr_freed = 0;
  int nr_moved = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

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

      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_present()) {
          nr_present++;
          obj_ptr.unlock(lock_id);
          auto allocator = LogAllocator::global_allocator();
          auto optptr = allocator->alloc_(obj_ptr.data_size(), true);
          lock_id = optptr->lock();
          assert(lock_id != -1 && !obj_ptr.null());

          if (optptr) {
            auto new_ptr = *optptr;
            ret = new_ptr.move_from(obj_ptr);
            if (ret == RetCode::Succ) {
              nr_moved++;
            } else if (ret == RetCode::Fail) {
              LOG(kError) << "Failed to move the object!";
              nr_failed++;
            } else
              goto faulted;
          } else {
            nr_failed++;
          }
        } else {
          nr_freed++;
        }

        pos += obj_size;
      }
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
          goto faulted;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_present: " << nr_present << ", nr_moved: " << nr_moved
              << ", nr_freed: " << nr_freed << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

bool LogChunk::free() {
  if (!sealed_)
    return false;

  int nr_non_present = 0;
  int nr_freed = 0;
  int nr_small_objs = 0;
  int nr_failed = 0;

  MetaObjectHdr hdr;

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

      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_present()) {
          if (obj_ptr.free(/* locked = */ true) == RetCode::Fault)
            goto faulted;
          nr_freed++;
        } else
          nr_non_present++;

        pos += obj_size;
      }
    } else { // TODO: large object
      LOG(kError) << "Not implemented yet!";
      exit(-1);
      auto opt_meta = load_hdr<MetaObjectHdr>(obj_ptr);
      if (!opt_meta)
        goto faulted;
      else {
        auto meta_hdr = *opt_meta;
        if (meta_hdr.is_continue()) {
          // this is a inner chunk storing a large object.
        } else {
          // this is the head chunk of a large object.
          goto faulted;
        }
      }
    }
    obj_ptr.unlock(lock_id);
    continue;
  faulted:
    nr_failed++;
    LOG(kError) << "chunk is unmapped under the hood";
    obj_ptr.unlock(lock_id);
    break;
  }
  LOG(kDebug) << "nr_freed: " << nr_freed
              << ", nr_non_present: " << nr_non_present
              << ", nr_failed: " << nr_failed;

  assert(nr_failed == 0);
  return nr_failed == 0;
}

/** LogRegion */
inline std::shared_ptr<LogChunk> LogRegion::allocChunk() {
  if (full()) {
    seal();
    return nullptr;
  }

  uint64_t addr = pos_;
  pos_ += kLogChunkSize;
  auto chunk = std::make_shared<LogChunk>(this, addr);
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
  if (!sealed_)
    return;
  alive_bytes_ = 0;
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

void LogRegion::free() {
  if (!sealed_)
    return;
  bool ret = true;
  for (auto &chunk : vLogChunks_) {
    ret &= chunk->free();
  }
  if (ret)
    destroy();
}

/** LogAllocator */
// must be called under lock protection
// try to get a non-empty region
inline std::shared_ptr<LogRegion> LogAllocator::getRegion() {
  if (!vRegions_.empty()) {
    auto region = vRegions_.back();
    if (!region->full())
      return region;
    region->seal();
  }
  return nullptr;
}

// alloc a new region
inline std::shared_ptr<LogRegion> LogAllocator::allocRegion(bool overcommit) {
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion(overcommit);
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);

  auto region = std::make_shared<LogRegion>(
      rid, reinterpret_cast<uint64_t>(range.stt_addr));

  return region;
}

inline std::shared_ptr<LogChunk> LogAllocator::allocChunk(bool overcommit) {
  std::unique_lock<std::mutex> ul(lock_);
  std::shared_ptr<LogChunk> chunk = nullptr;
  auto region = getRegion();
  if (region) {
    chunk = region->allocChunk();
    if (chunk != nullptr)
      return chunk;
  }

  ul.unlock();
  region = allocRegion(overcommit);
  ul.lock();
  if (!region)
    return nullptr;
  vRegions_.push_back(region);
  return region->allocChunk();
}

std::optional<ObjectPtr> LogAllocator::alloc_(size_t size, bool overcommit) {
  size = round_up_to_align(size, kSmallObjSizeUnit);
  if (size >= kSmallObjThreshold) { // large obj
    // LOG(kError) << "large obj allocation is not implemented yet!";
    return alloc_large(size);
  }

  if (pcab.get()) {
    auto ret = pcab->alloc(size);
    if (ret)
      return ret;
    pcab.reset();
  }
  // slowpath
  auto chunk = allocChunk(overcommit);
  if (!chunk)
    return std::nullopt;

  pcab = chunk;
  auto ret = pcab->alloc(size);
  assert(ret);
  return ret;
}

// Large objects
std::optional<ObjectPtr> LogAllocator::alloc_large(size_t size) {
  constexpr static size_t kLogChunkAvailSize =
      kLogChunkSize - sizeof(LargeObjectHdr);
  assert(size >= kSmallObjThreshold);

  ObjectPtr obj_ptr;

  size_t nr_chunks = (size + kLogChunkAvailSize - 1) / kLogChunkAvailSize;
  std::vector<std::shared_ptr<LogRegion>> regions;
  std::vector<std::shared_ptr<LogChunk>> chunks;
  LogChunk *head_chunk = nullptr;

  auto region = allocRegion();
  if (!region)
    goto failed;
  regions.push_back(region);

  while (chunks.size() < nr_chunks) {
    auto chunk = region->allocChunk();
    if (!chunk) {
      region = allocRegion();
      if (!region)
        goto failed;
      regions.push_back(region);
      chunk = region->allocChunk();
      if (!chunk)
        goto failed;
    }
    chunks.push_back(chunk);
  }

  for (int i = 0; i < nr_chunks; i++) {
    const bool tail = i == nr_chunks - 1;
    auto chunk = chunks[i];
    LargeObjectHdr hdr;
    if (!head_chunk) {
      head_chunk = chunk.get();
      hdr.init(size);
    } else {
      hdr.init(std::min(kLogChunkAvailSize, size - i * kLogChunkAvailSize));
      auto meta_hdr = reinterpret_cast<MetaObjectHdr *>(&hdr);
      meta_hdr->set_continue();
      hdr.set_rref(
          reinterpret_cast<uint64_t>(head_chunk)); // treat as ref to head
    }
    if (!tail) {
      hdr.set_next(chunks[i + 1]->start_addr_);
    } else {
      hdr.set_next(0);
    }

    TransientPtr tptr(chunk->pos_, sizeof(hdr));
    if (!tptr.copy_from(&hdr, sizeof(hdr)))
      goto failed;
    chunk->pos_ += sizeof(hdr);
    if (size - i * kLogChunkAvailSize >= kLogChunkAvailSize)
      chunk->seal();
  }

  assert(head_chunk);
  if (obj_ptr.init_from_soft(head_chunk->start_addr_) != RetCode::Succ)
    goto failed;

  lock_.lock();
  for (auto region : regions)
    vRegions_.push_back(region);
  lock_.unlock();
  return obj_ptr;

failed:
  for (auto region : regions)
    region->destroy();

  return std::nullopt;
}

// Define PCAB
thread_local std::shared_ptr<LogChunk> LogAllocator::pcab;

} // namespace cachebank
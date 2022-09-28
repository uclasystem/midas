#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>

#include "resource_manager.hpp"
#include "utils.hpp"
#include "transient_ptr.hpp"

namespace cachebank {

class LogChunk {
public:
  LogChunk(uint64_t start_addr);
  TransientPtr alloc(size_t size);
  bool free(uint64_t ptr);

private:
  void init(uint64_t start_addr);
  void iterate(size_t pos);

  constexpr static uint32_t kLogChunkSize = kPageChunkSize;
  static_assert(kRegionSize % kLogChunkSize == 0,
                "Region size must be multiple chunk size");
  constexpr static uint32_t kObjHdrSize = 16; // TODO: reduce it
  static_assert(kObjHdrSize >= sizeof(ObjectHdr),
                "Object header size must large than sizeof(ObjectHdr)");

  std::mutex lock_;
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogRegion {
public:
  LogRegion(uint64_t addr) : start_addr_(addr), pos_(addr) { init(); }
  uint64_t allocChunk();

  inline bool full() const noexcept { return pos_ == start_addr_ + kRegionSize; }
  inline uint32_t size() const noexcept { return pos_ / kPageChunkSize; }

  void evacuate(LogRegion *dst) {}

private:
  uint32_t init() { return 0; };
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogAllocator {
public:
  LogAllocator();
  TransientPtr alloc(size_t size);
  bool free(TransientPtr &ptr);

private:
  bool allocRegion();
  bool allocChunk();

  std::mutex lock_;
  std::vector<std::shared_ptr<LogRegion>> vRegions_;
  std::vector<std::shared_ptr<LogChunk>> vLogChunks_;
  std::atomic_int32_t curr_region_;
  std::atomic_int32_t curr_chunk_;

  // Per Core Allocation Buffer
  // YIFAN: currently implemented as thread local buffers
  static thread_local std::shared_ptr<LogChunk> pcab;
};

} // namespace cachebank

#include "impl/log.ipp"

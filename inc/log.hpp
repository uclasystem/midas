#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/types.h>
#include <vector>
#include <optional>

#include "resource_manager.hpp"
#include "utils.hpp"
#include "transient_ptr.hpp"

namespace cachebank {

class LogChunk {
public:
  LogChunk(uint64_t start_addr);
  std::optional<TransientPtr> alloc(size_t size);
  bool free(uint64_t ptr);
  void seal() noexcept;
  bool full() const noexcept;

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
  bool full_;
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogRegion {
public:
  LogRegion(uint64_t addr);
  uint64_t allocChunk();

  inline bool full() const noexcept;
  inline uint32_t size() const noexcept;

  void evacuate(LogRegion *dst) {}

private:
  void init();
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogAllocator {
public:
  LogAllocator();
  std::optional<TransientPtr> alloc(size_t size);
  bool free(TransientPtr &ptr);

private:
  bool allocRegion();
  bool allocChunk();
  bool _allocChunk();

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

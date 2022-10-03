#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>
#include <mutex>
#include <memory>

#include "object.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

namespace cachebank {

class LogChunk {
public:
  LogChunk(uint64_t addr);
  std::optional<TransientPtr> alloc(size_t size);
  bool free(uint64_t ptr);
  void seal() noexcept;
  bool full() noexcept;

private:
  void init(uint64_t addr);
  void iterate(size_t pos);
  void scan();
  void evacuate();

  friend class Evacuator;

  static_assert(kRegionSize % kLogChunkSize == 0,
                "Region size must be multiple chunk size");

  // std::mutex lock_;
  bool sealed_;
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogRegion {
public:
  LogRegion(uint64_t addr);
  uint64_t allocChunk();

  bool full() noexcept;
  uint32_t size() const noexcept;
  void seal() noexcept;

private:
  void init();
  void scan();
  void evacuate();

  friend class Evacuator;

  bool sealed_;
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogAllocator {
public:
  LogAllocator();
  std::optional<TransientPtr> alloc(size_t size);
  bool free(TransientPtr &ptr);

  static inline LogAllocator *global_allocator() noexcept;

private:
  constexpr static uint32_t kSmallObjThreshold = 8 << 12;

  std::shared_ptr<LogRegion> getRegion();
  std::shared_ptr<LogChunk> allocChunk();

  std::mutex lock_;
  std::vector<std::shared_ptr<LogRegion>> vRegions_;
  std::vector<std::shared_ptr<LogChunk>> vLogChunks_;
  std::atomic_int32_t curr_region_;
  std::atomic_int32_t curr_chunk_;

  friend class Evacuator;

  // Per Core Allocation Buffer
  // YIFAN: currently implemented as thread local buffers
  static thread_local std::shared_ptr<LogChunk> pcab;
};

} // namespace cachebank

#include "impl/log.ipp"

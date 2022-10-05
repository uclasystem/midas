#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <list>

#include "object.hpp"
#include "utils.hpp"

namespace cachebank {

class LogChunk {
public:
  LogChunk(uint64_t addr);
  std::optional<ObjectPtr> alloc(size_t size);
  bool free(ObjectPtr &ptr);
  void seal() noexcept;
  bool full() noexcept;

  bool scan();
  bool evacuate();

private:
  void init(uint64_t addr);
  void iterate(size_t pos);

  static_assert(kRegionSize % kLogChunkSize == 0,
                "Region size must be multiple chunk size");

  // std::mutex lock_;
  bool sealed_;
  uint64_t start_addr_;
  uint64_t pos_;
};

class LogRegion {
public:
  LogRegion(int64_t rid, uint64_t addr);
  std::shared_ptr<LogChunk> allocChunk();

  bool destroyed() const noexcept;
  bool full() const noexcept;
  uint32_t size() const noexcept;
  void seal() noexcept;
  void destroy();

  void scan();
  void evacuate();

private:
  int64_t region_id_;
  uint64_t start_addr_;
  uint64_t pos_;
  bool sealed_;
  bool destroyed_;

  std::list<std::shared_ptr<LogChunk>> vLogChunks_;
};

class LogAllocator {
public:
  LogAllocator();
  std::optional<ObjectPtr> alloc(size_t size);
  bool free(ObjectPtr &ptr);

  static inline LogAllocator *global_allocator() noexcept;

private:
  std::shared_ptr<LogRegion> getRegion();
  std::shared_ptr<LogChunk> allocChunk();

  std::mutex lock_;
  std::list<std::shared_ptr<LogRegion>> vRegions_;
  std::atomic_int32_t curr_region_;
  std::atomic_int32_t curr_chunk_;

  template <int nr_thds> friend class Evacuator;
  void cleanup_regions();

  // Per Core Allocation Buffer
  // YIFAN: currently implemented as thread local buffers
  static thread_local std::shared_ptr<LogChunk> pcab;
};

} // namespace cachebank

#include "impl/log.ipp"

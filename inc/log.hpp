#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <optional>

#include "object.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

namespace cachebank {

class LogSegment;

class LogChunk {
public:
  LogChunk(LogSegment *segment, uint64_t addr);
  std::optional<ObjectPtr> alloc_small(size_t size);
  std::optional<std::pair<TransientPtr, size_t>>
  alloc_large(size_t size, const TransientPtr head_addr, TransientPtr prev_addr);
  bool free(ObjectPtr &ptr);
  void seal() noexcept;
  bool full() const noexcept;
  int32_t remaining_bytes() const noexcept;

private:
  void init(uint64_t addr);
  void iterate(size_t pos);

  void set_alive_bytes(int32_t alive_bytes) noexcept;

  static_assert(kRegionSize % kLogChunkSize == 0,
                "Region size must be multiple chunk size");

  /** Belonging segment */
  LogSegment *segment_;

  bool sealed_;
  int32_t alive_bytes_;
  uint64_t start_addr_;
  uint64_t pos_;

  friend class Evacuator;
  friend class LogAllocator;
};

class LogSegment {
public:
  LogSegment(int64_t rid, uint64_t addr);
  std::shared_ptr<LogChunk> allocChunk();

  bool destroyed() const noexcept;
  bool full() const noexcept;
  uint32_t size() const noexcept;
  void seal() noexcept;
  void destroy();

  float get_alive_ratio() const noexcept;

private:
  std::mutex mtx_;
  int32_t alive_bytes_;

  int64_t region_id_;
  uint64_t start_addr_;
  uint64_t pos_;
  bool sealed_;
  bool destroyed_;

  std::list<std::shared_ptr<LogChunk>> vLogChunks_;

  friend class LogChunk;
  friend class Evacuator;
};

class LogAllocator {
public:
  LogAllocator();
  std::optional<ObjectPtr> alloc(size_t size);
  bool alloc_to(size_t size, ObjectPtr *dst);
  bool free(ObjectPtr &ptr);

  // accessing internal counters
  static inline int64_t total_access_cnt() noexcept;
  static inline void reset_access_cnt() noexcept;
  static inline int64_t total_alive_cnt() noexcept;
  static inline void reset_alive_cnt() noexcept;
  static inline void count_access();
  static inline void count_alive(int val);

  static inline void thd_exit();

  static inline LogAllocator *global_allocator() noexcept;

private:
  std::optional<ObjectPtr> alloc_(size_t size, bool overcommit);
  std::optional<ObjectPtr> alloc_large(size_t size, bool overcommit);
  std::shared_ptr<LogChunk> getChunk();
  std::shared_ptr<LogSegment> getSegment();
  std::shared_ptr<LogSegment> allocSegment(bool overcommit = false);
  std::shared_ptr<LogChunk> allocChunk(bool overcommit = false);

  /** Allocation */
  std::mutex lock_;
  std::list<std::shared_ptr<LogSegment>> vSegments_;
  std::atomic_int32_t curr_segment_;
  std::atomic_int32_t curr_chunk_;

  /** Counters */
  static std::atomic_int64_t total_access_cnt_;
  static std::atomic_int64_t total_alive_cnt_;
  static void signal_scanner();

  friend class Evacuator;
  friend class LogChunk;
  int cleanup_segments();

  static inline void seal_pcab();

  /** Thread-local variables */
  // Per Core Allocation Buffer
  // YIFAN: currently implemented as thread local buffers
  static thread_local std::shared_ptr<LogChunk> pcab;
  static thread_local int32_t access_cnt_;
  static thread_local int32_t alive_cnt_;
};

} // namespace cachebank

#include "impl/log.ipp"

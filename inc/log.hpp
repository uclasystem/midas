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

class LogAllocator;

class LogSegment {
public:
  LogSegment(LogAllocator *owner, int64_t rid, uint64_t addr);
  std::optional<ObjectPtr> alloc_small(size_t size);
  std::optional<std::pair<TransientPtr, size_t>>
  alloc_large(size_t size, const TransientPtr head_addr,
              TransientPtr prev_addr);
  bool free(ObjectPtr &ptr);
  void seal() noexcept;
  void destroy() noexcept;

  uint32_t size() const noexcept;
  bool sealed() const noexcept;
  bool destroyed() const noexcept;
  bool full() const noexcept;
  int32_t remaining_bytes() const noexcept;
  float get_alive_ratio() const noexcept;

private:
  void init(uint64_t addr);
  void iterate(size_t pos);

  void set_alive_bytes(int32_t alive_bytes) noexcept;

  static_assert(kRegionSize % kLogSegmentSize == 0,
                "Region size must equal to segment size");

  LogAllocator *owner_;
  bool sealed_;
  bool destroyed_;
  int32_t alive_bytes_;
  uint64_t start_addr_;
  uint64_t pos_;

  int64_t region_id_;

  friend class Evacuator;
  friend class LogAllocator;
};

class SegmentList {
public:
  void push_back(std::shared_ptr<LogSegment> segment);
  std::shared_ptr<LogSegment> pop_front();
  bool empty() const noexcept;

private:
  std::mutex lock_;
  std::list<std::shared_ptr<LogSegment>> segments_;
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
  std::shared_ptr<LogSegment> allocSegment(bool overcommit = false);

  /** Allocation */
  SegmentList segments_;
  SegmentList stashed_pcabs_;

  /** Counters */
  static std::atomic_int64_t total_access_cnt_;
  static std::atomic_int64_t total_alive_cnt_;
  static void signal_scanner();

  friend class Evacuator;
  friend class LogSegment;

  /** Thread-local variables */
  // Per Core Allocation Buffer
  // YIFAN: currently implemented as thread local buffers
  static thread_local std::shared_ptr<LogSegment> pcab;
  static thread_local int32_t access_cnt_;
  static thread_local int32_t alive_cnt_;
};

} // namespace cachebank

#include "impl/log.ipp"

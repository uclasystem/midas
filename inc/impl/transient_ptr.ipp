#pragma once

#include <cstring>

namespace cachebank {

inline TransientPtr::TransientPtr() : ptr_(0), size_(0) {}

inline TransientPtr::TransientPtr(uint64_t addr, size_t size)
    : ptr_(addr), size_(size) {}

inline bool TransientPtr::is_valid() const { return ptr_; }

inline bool TransientPtr::set(uint64_t addr, size_t size) {
  // TODO: page-fault-aware logic
  // if (!isValid(addr)) return false;
  ptr_ = addr;
  size_ = size;
  return true;
}

inline bool TransientPtr::reset() noexcept {
  ptr_ = 0;
  size_ = 0;
  return true;
}

inline size_t TransientPtr::size() const noexcept { return size_; }

inline TransientPtr TransientPtr::slice(int64_t offset, size_t size) const {
  return is_valid() ? TransientPtr(ptr_ + offset, size) : TransientPtr();
}

/**
 * Atomic operations
 */
inline bool TransientPtr::cmpxchg(int64_t offset, uint64_t oldval,
                                  uint64_t newval) {
  auto addr = reinterpret_cast<uint64_t *>(ptr_ + offset);
  return __sync_val_compare_and_swap(addr, oldval, newval);
}

inline int64_t TransientPtr::atomic_add(int64_t offset, int64_t val) {
  auto addr = reinterpret_cast<uint64_t *>(ptr_ + offset);
  return __sync_fetch_and_add(addr, val);
}

inline bool TransientPtr::copy_from(const void *src, size_t len,
                                    int64_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(reinterpret_cast<void *>(ptr_ + offset), src, len);
  return true;
}

inline bool TransientPtr::copy_to(void *dst, size_t len, int64_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(dst, reinterpret_cast<void *>(ptr_ + offset), len);
  return true;
}

inline bool TransientPtr::copy_from(const TransientPtr &src, size_t len,
                                    int64_t from_offset, int64_t to_offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (from_offset + len > src.size_ || to_offset + len > this->size_)
    return false;
  std::memcpy(reinterpret_cast<void *>(this->ptr_ + to_offset),
              reinterpret_cast<void *>(src.ptr_ + from_offset), len);
  return true;
}

inline bool TransientPtr::copy_to(TransientPtr &dst, size_t len,
                                  int64_t from_offset, int64_t to_offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (from_offset + len > dst.size_ || to_offset + len > this->size_)
    return false;
  std::memcpy(reinterpret_cast<void *>(dst.ptr_ + to_offset),
              reinterpret_cast<void *>(this->ptr_ + from_offset), len);
  return true;
}

inline bool TransientPtr::assign_to_non_volatile(TransientPtr *dst) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  *dst = *this;
  return true;
}

inline bool TransientPtr::assign_to_local_region(TransientPtr *dst) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  *dst = *this;
  return true;
}

inline bool TransientPtr::assign_to_foreign_region(TransientPtr *dst) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  *dst = *this;
  return true;
}
} // namespace cachebank
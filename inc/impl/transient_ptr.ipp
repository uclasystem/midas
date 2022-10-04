#pragma once

#include <cstring>

namespace cachebank {

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

inline bool TransientPtr::copy_from(const void *src, size_t len,
                                    size_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(reinterpret_cast<void *>(ptr_ + offset), src, len);
  return true;
}

inline bool TransientPtr::copy_to(void *dst, size_t len, size_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(dst, reinterpret_cast<void *>(ptr_ + offset), len);
  return true;
}

inline bool TransientPtr::copy_from(const TransientPtr &src, size_t len,
                                    size_t from_offset, size_t to_offset) {
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
                                  size_t from_offset, size_t to_offset) {
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
#pragma once

#include <cstddef>
#include <cstring>

#include "object.hpp"
#include "utils.hpp"

namespace cachebank {

inline TransientPtr::TransientPtr(void *ptr, size_t size)
    : ptr_(ptr), size_(size) {}

inline bool TransientPtr::is_valid() const { return ptr_ != nullptr; }

inline bool TransientPtr::set(void *ptr, size_t size) {
  // TODO: page-fault-aware logic
  // if (!isValid(ptr)) return false;
  ptr_ = ptr;
  size_ = size;
  return true;
}

inline bool TransientPtr::reset() noexcept {
  ptr_ = nullptr;
  size_ = 0;
  return true;
}

inline TransientPtr TransientPtr::slice(int64_t offset, size_t size) const {
  if (!is_valid())
    return TransientPtr();
  auto new_addr =
      reinterpret_cast<void *>(reinterpret_cast<uint64_t>(ptr_) + offset);
  return TransientPtr(new_addr, size);
}

inline bool TransientPtr::copy_from(void *src, size_t len, size_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(ptr_offset(ptr_, offset), src, len);
  return true;
}

inline bool TransientPtr::copy_to(void *dst, size_t len, size_t offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (offset + len > size_)
    return false;
  std::memcpy(dst, ptr_offset(ptr_, offset), len);
  return true;
}

inline bool TransientPtr::copy_from(TransientPtr &src, size_t len,
                                    size_t from_offset, size_t to_offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (from_offset + len > src.size_ || to_offset + len > this->size_)
    return false;
  std::memcpy(ptr_offset(this->ptr_, to_offset),
              ptr_offset(src.ptr_, from_offset), len);
  return true;
}

inline bool TransientPtr::copy_to(TransientPtr &dst, size_t len,
                                  size_t from_offset, size_t to_offset) {
  if (!is_valid())
    return false;
  // TODO: page-fault-aware logic
  if (from_offset + len > dst.size_ || to_offset + len > this->size_)
    return false;
  std::memcpy(ptr_offset(dst.ptr_, to_offset),
              ptr_offset(this->ptr_, from_offset), len);
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
#pragma once

#include "soft_unique_ptr.hpp"
#include <algorithm>
#include <cstdint>

namespace midas {

template <typename T, typename... ReconArgs>
using SoftMemoryPool = SoftMemPool<T, ReconArgs...>;

template <typename T, typename... ReconArgs>
SoftUniquePtr<T, ReconArgs...>::SoftUniquePtr(SoftMemoryPool *pool) noexcept
    : ptr_(), pool_(pool) {
  assert(pool_);
}

template <typename T, typename... ReconArgs>
SoftUniquePtr<T, ReconArgs...>::~SoftUniquePtr() {
  if (!ptr_.null())
    ptr_.free();
  ptr_.reset();
}

template <typename T, typename... ReconArgs>
SoftUniquePtr<T, ReconArgs...>::SoftUniquePtr(
    SoftUniquePtr<T, ReconArgs...> &&other) noexcept
    : pool_(other.pool_) {
  ptr_ = other.ptr_;
  if (!ptr_.null())
    ptr_.set_rref(&this->ptr_);
  other.ptr_.reset();
}

template <typename T, typename... ReconArgs>
SoftUniquePtr<T, ReconArgs...> &SoftUniquePtr<T, ReconArgs...>::operator=(
    SoftUniquePtr<T, ReconArgs...> &&other) noexcept {
  if (this != &other) {
    if (!ptr_.null())
      ptr_.free();
    pool_ = other.pool_;
    ptr_ = other.ptr_;
    if (!ptr_.null())
      ptr_.set_rref(&this->ptr_);
    other.ptr_.reset();
  }
  return *this;
}

template <typename T, typename... ReconArgs>
T SoftUniquePtr<T, ReconArgs...>::read(ReconArgs... args) {
  T value;
  if (ptr_.null() || !ptr_.copy_to(&value, sizeof(T)))
    value = pool_->reconstruct(args...);
  return value;
}

template <typename T, typename... ReconArgs>
void SoftUniquePtr<T, ReconArgs...>::write(const T &value) {
  if (ptr_.null())
    pool_->alloc_to(sizeof(T), &ptr_);
  ptr_.copy_from(&value, sizeof(T));
}

template <typename T, typename... ReconArgs>
bool SoftUniquePtr<T, ReconArgs...>::cmpxchg(const T &oldval, const T &newval) {
  if (ptr_.null())
    return false;

  bool succ = true;
  int64_t offset = 0;
  while (offset < sizeof(T)) {
    const uint64_t SIZE = std::min(sizeof(T) - offset, sizeof(uint64_t));
    const uint64_t MASK = (1 << (SIZE * sizeof(uint64_t))) - 1;
    // NOTE (YIFAN): WARNING! Below may induce out-of-bound read when sizeof(T)
    // is not a multiple of sizeof(uint64_t). This is typically not a problem
    // because cmpxchg is usually used for aligned objects, but we should fix it
    // in the future.
    const uint64_t oldval_ =
        *(reinterpret_cast<const uint64_t *>(&oldval) + offset) & MASK;
    const uint64_t newval_ =
        *(reinterpret_cast<const uint64_t *>(&newval) + offset) & MASK;
    if (!ptr_.cmpxchg(offset, oldval_, newval_)) {
      succ = false;
      break;
    }
    offset += sizeof(uint64_t);
  }
  return succ;
}

template <typename T, typename... ReconArgs>
void SoftUniquePtr<T, ReconArgs...>::reset() {
  if (!ptr_.null())
    ptr_.free();
  ptr_.reset();
}

} // namespace midas
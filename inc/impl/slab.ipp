#pragma once

#ifdef ENABLE_SLAB

namespace cachebank {
inline uint32_t SlabAllocator::get_slab_idx(uint32_t size) noexcept {
  uint32_t rounded_size = utils::round_up_power_of_two(size);
  assert(rounded_size <= kMaxSlabSize);
  return utils::bsr_32(std::max(rounded_size >> kMinSlabShift, 1u));
}

inline uint32_t SlabAllocator::get_slab_size(uint32_t idx) noexcept {
  return kMinSlabSize << idx;
}

inline void *SlabAllocator::alloc(uint32_t size) { return _alloc(size); }

template <typename T> T *SlabAllocator::alloc(uint32_t cnt) {
  return reinterpret_cast<T *>(_alloc(sizeof(T) * cnt));
}

/* A thread safe way to create a global allocator and get its reference. */
inline SlabAllocator *SlabAllocator::global_allocator() {
  static std::mutex mtx_;
  static std::unique_ptr<SlabAllocator> _allocator(nullptr);

  if (LIKELY(_allocator.get() != nullptr))
    return _allocator.get();

  std::unique_lock<std::mutex> lk(mtx_);
  if (UNLIKELY(_allocator.get() != nullptr))
    return _allocator.get();

  _allocator = std::make_unique<SlabAllocator>();
  return _allocator.get();
}
} // namespace cachebank

#endif // ENABLE_SLAB
#pragma once

namespace cachebank {

inline LogChunk::LogChunk(uint64_t addr)
    : start_addr_(addr), pos_(addr), sealed_(false) {}

inline void LogChunk::seal() noexcept {
  assert(!full());
  GenericObjectHdr endHdr;
  endHdr.set_invalid();
  auto endPtr = TransientPtr(reinterpret_cast<GenericObjectHdr *>(pos_),
                             sizeof(GenericObjectHdr));
  endPtr.copy_from(&endHdr, sizeof(endPtr)); // ignore return value
  sealed_ = true;
}

inline bool LogChunk::full() noexcept {
  return pos_ + sizeof(GenericObjectHdr) >= start_addr_ + kPageChunkSize;
}

inline LogRegion::LogRegion(uint64_t addr)
    : start_addr_(addr), pos_(addr), sealed_(false) {}

inline void LogRegion::seal() noexcept {
  assert(!full());
  sealed_ = true;
}

inline bool LogRegion::full() noexcept {
  return pos_ >= start_addr_ + kRegionSize;
}

inline uint32_t LogRegion::size() const noexcept {
  return pos_ / kPageChunkSize;
}

inline void LogRegion::init() {}

inline LogAllocator::LogAllocator() : curr_region_(0), curr_chunk_(0) {}

/* A thread safe way to create a global allocator and get its reference. */
inline LogAllocator *LogAllocator::global_allocator() noexcept {
  static std::mutex _mtx;
  static std::unique_ptr<LogAllocator> _allocator(nullptr);

  if (likely(_allocator.get() != nullptr))
    return _allocator.get();

  std::unique_lock<std::mutex> lk(_mtx);
  if (unlikely(_allocator.get() != nullptr))
    return _allocator.get();

  _allocator = std::make_unique<LogAllocator>();
  return _allocator.get();
}

} // namespace cachebank
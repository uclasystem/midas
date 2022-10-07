#pragma once

namespace cachebank {

/** LogChunk */
inline LogChunk::LogChunk(uint64_t addr)
    : start_addr_(addr), pos_(addr), sealed_(false) {}

inline void LogChunk::seal() noexcept {
  if (sealed_)
    return;
  GenericObjectHdr endHdr;
  endHdr.set_invalid();
  auto endPtr = TransientPtr(pos_, sizeof(GenericObjectHdr));
  endPtr.copy_from(&endHdr, sizeof(endHdr)); // ignore return value
  pos_ += sizeof(GenericObjectHdr);
  sealed_ = true;
}

inline bool LogChunk::full() noexcept {
  return sealed_ ||
         pos_ + sizeof(GenericObjectHdr) > start_addr_ + kLogChunkSize;
}

/** LogRegion */
inline LogRegion::LogRegion(int64_t rid, uint64_t addr)
    : region_id_(rid), start_addr_(addr), pos_(addr), sealed_(false),
      destroyed_(false) {}

inline void LogRegion::seal() noexcept { sealed_ = true; }

inline bool LogRegion::destroyed() const noexcept { return destroyed_; }

inline bool LogRegion::full() const noexcept {
  return pos_ >= start_addr_ + kRegionSize;
}

inline uint32_t LogRegion::size() const noexcept { return pos_ / kRegionSize; }

/** LogAllocator */
inline LogAllocator::LogAllocator() : curr_region_(0), curr_chunk_(0) {}

inline void LogAllocator::cleanup_regions() {
  for (auto rit = vRegions_.begin(); rit != vRegions_.end();) {
    if ((*rit)->destroyed())
      rit = vRegions_.erase(rit);
    else
      rit++;
  }
}

inline void LogAllocator::seal_pcab() {
  if (pcab.get())
    pcab->seal();
}

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

static thread_local struct ThreadExiter {
  ~ThreadExiter() { LogAllocator::seal_pcab(); }
} exiter;

} // namespace cachebank
#pragma once

namespace cachebank {

inline LogChunk::LogChunk(uint64_t start_addr)
    : start_addr_(start_addr), pos_(start_addr) {}

inline void LogChunk::seal() noexcept { full_ = true; }

inline bool LogChunk::full() const noexcept { return full_; }

inline LogRegion::LogRegion(uint64_t addr) : start_addr_(addr), pos_(addr) {}

inline bool LogRegion::full() const noexcept {
  return pos_ >= start_addr_ + kRegionSize;
}
inline uint32_t LogRegion::size() const noexcept {
  return pos_ / kPageChunkSize;
}

inline void LogRegion::init() {}

inline LogAllocator::LogAllocator() : curr_region_(0), curr_chunk_(0) {}

} // namespace cachebank
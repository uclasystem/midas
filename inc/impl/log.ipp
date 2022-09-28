#pragma once

namespace cachebank {

inline LogChunk::LogChunk(uint64_t start_addr)
    : start_addr_(start_addr), pos_(start_addr) {}

inline LogAllocator::LogAllocator() : curr_region_(0), curr_chunk_(0) {}

} // namespace cachebank
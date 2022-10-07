#pragma once

#include <cstdint>

namespace cachebank {

constexpr static uint32_t kNumCPUs = 128;
constexpr static uint32_t kSmallObjSizeUnit = sizeof(uint64_t);

constexpr static uint32_t kShmObjNameLen = 128;
constexpr static uint32_t kPageSize = 4096;            // 4KB
constexpr static uint32_t kPageChunkSize = 512 * 4096; // 2MB == Huge Page
constexpr static uint64_t kVolatileSttAddr = 0x01f'000'000'000;

// log structured allocator related
constexpr static uint32_t kLogChunkSize = kPageChunkSize;
constexpr static uint32_t kRegionSize = kLogChunkSize << 2;
constexpr static uint64_t kRegionMask = ~(kRegionSize - 1ull);

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

// align must be power of 2.
#define round_up_to_align(val, align) (((val) + ((align)-1)) & ~((align)-1))
#define ptr_offset(ptr, offset) (reinterpret_cast<char *>(ptr) + (offset))

} // namespace cachebank
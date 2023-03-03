#pragma once

#include <cstdint>
#include <limits>

namespace midas {

constexpr static uint32_t kCPUFreq = 2600;
constexpr static uint32_t kNumCPUs = 128;
constexpr static uint32_t kSmallObjSizeUnit = sizeof(uint64_t);

constexpr static uint32_t kShmObjNameLen = 128;
constexpr static uint32_t kPageSize = 4096;                // 4KB
constexpr static uint32_t kHugePageSize = 512 * kPageSize; // 2MB == Huge Page
constexpr static uint64_t kVolatileSttAddr = 0x01f'000'000'000;
constexpr static uint64_t kVolatileEndAddr =
    0x01f'000'000'000 + 0x040'000'000'000;

/** Log Structured Allocator related */
constexpr static uint32_t kLogSegmentSize = kHugePageSize;
constexpr static uint64_t kLogSegmentMask = ~(kLogSegmentSize - 1ull);
constexpr static uint32_t kRegionSize = kLogSegmentSize;
constexpr static uint64_t kRegionMask = ~(kRegionSize - 1ull);

constexpr static int32_t kMaxAliveBytes = std::numeric_limits<int32_t>::max();
/** Evacuator related */
constexpr static float kAliveThreshHigh = 0.9;

#define LIKELY(x) __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)

// align must be power of 2.
#define round_up_to_align(val, align) (((val) + ((align)-1)) & ~((align)-1))
#define round_to_align(val, align) ((val) & ~((align)-1))
#define ptr_offset(ptr, offset) (reinterpret_cast<char *>(ptr) + (offset))

#ifdef DEBUG
#define FORCE_INLINE inline
#else
#define FORCE_INLINE inline __attribute__((always_inline))
#endif

#define NOINLINE __attribute__((noinline))
#define FLATTEN __attribute__((flatten))
#define SOFT_RESILIENT __attribute__((section("resilient-func")))

#define DECL_RESILIENT_FUNC(ret_type, func, ...)                               \
  ret_type FLATTEN NOINLINE func(__VA_ARGS__) SOFT_RESILIENT;                  \
  void func##_end() SOFT_RESILIENT;

#define DELIM_FUNC_IMPL(func)                                                  \
  void func##_end() { asm volatile(".byte 0xcc, 0xcc, 0xcc, 0xcc"); }

} // namespace midas
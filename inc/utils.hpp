#pragma once

#include <cstdint>
#include <string>

namespace cachebank {

constexpr static uint32_t kNumCPUs = 128;

constexpr static uint32_t kDaemonQDepth = 1024;
constexpr static uint32_t kClientQDepth = 128;
constexpr static char kNameCtrlQ[] = "daemon_ctrl_mq";

constexpr static uint32_t kShmObjNameLen = 128;
constexpr static uint32_t kPageSize = 4096;            // 4KB
constexpr static uint32_t kPageChunkSize = 512 * 4096; // 2MB == Huge Page
constexpr static uint32_t kRegionSize = kPageChunkSize;
constexpr static uint64_t kPageChunkAlignMask = ~(kPageChunkSize - 1ull);
constexpr static uint64_t kVolatileSttAddr = 0x01f'000'000'000;

enum CtrlOpCode {
  CONNECT,
  DISCONNECT,
  ALLOC,
  FREE,
};

enum CtrlRetCode {
  CONN_SUCC,
  CONN_FAIL,
  MEM_SUCC,
  MEM_FAIL,
};

enum ClientStatusCode {
  INIT,
  CONNECTED,
  DISCONNECTED,
};

struct MemMsg {
  int64_t region_id;
  int64_t size;
};

struct CtrlMsg {
  uint64_t id;
  CtrlOpCode op;
  CtrlRetCode ret;
  MemMsg mmsg;
};

struct VRange {
  VRange() = default;
  VRange(void *addr_, size_t size_) : stt_addr(addr_), size(size_) {}

  void *stt_addr;
  size_t size;

  bool contains(const void *ptr) const noexcept {
    return ptr > stt_addr && ptr < reinterpret_cast<char *>(stt_addr) + size;
  }
};

#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)

// align must be power of 2.
#define round_up_to_align(val, align) (((val) + ((align)-1)) & ~((align)-1))
#define ptr_offset(ptr, offset) (reinterpret_cast<char *>(ptr) + (offset))

#define get_bit32(word, bit) ((word) & (reinterpret_cast<uint32_t>(1) << (bit)))
#define set_bit32(word, bit)                                                   \
  ((word) | ~(reinterpret_cast<uint32_t>(1) << (bit)))
#define clr_bit32(word, bit)                                                   \
  ((word) & ~(reinterpret_cast<uint32_t>(1) << (bit)))

namespace utils {

static inline const std::string get_sendq_name(uint64_t id) {
  return "sendq-" + std::to_string(id);
}

static inline const std::string get_recvq_name(uint64_t id) {
  return "recvq-" + std::to_string(id);
}

static inline const std::string get_region_name(uint64_t pid, uint64_t rid) {
  return "region-" + std::to_string(pid) + "-" + std::to_string(rid);
}

/* From AIFM */
static inline uint32_t bsr_32(uint32_t a) {
  uint32_t ret;
  asm("BSR %k1, %k0 \n" : "=r"(ret) : "rm"(a));
  return ret;
}

static inline uint64_t bsr_64(uint64_t a) {
  uint64_t ret;
  asm("BSR %q1, %q0 \n" : "=r"(ret) : "rm"(a));
  return ret;
}

static inline constexpr uint32_t round_up_power_of_two(uint32_t a) {
  return a == 1 ? 1 : 1 << (32 - __builtin_clz(a - 1));
}

} // namespace utils
} // namespace cachebank
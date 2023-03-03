#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace midas {

enum CtrlOpCode {
  CONNECT,
  DISCONNECT,
  ALLOC,
  OVERCOMMIT,
  FREE,
  UPDLIMIT,
  UPDLIMIT_REQ,
};

enum CtrlRetCode {
  CONN_SUCC,
  CONN_FAIL,
  MEM_SUCC,
  MEM_FAIL,
};

struct MemMsg {
  int64_t region_id;
  uint64_t size;
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

namespace utils {
static inline const std::string get_region_name(uint64_t pid, uint64_t rid) {
  return "region-" + std::to_string(pid) + "-" + std::to_string(rid);
}
} // namespace utils

} // namespace midas
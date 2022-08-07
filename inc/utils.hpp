#pragma once

#include <cstdint>
#include <sys/types.h>
#include <string>

namespace cachebank {

constexpr static uint32_t kDaemonQDepth = 1024;
constexpr static uint32_t kClientQDepth = 128;
constexpr static char kNameCtrlQ[] = "daemon_ctrl_mq";

constexpr static uint32_t kShmObjNameLen = 128;
constexpr static uint32_t kPageSize = 4096; // 4KB
constexpr static uint32_t kPageChunkSize = 512 * 4096; // 2MB

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
  uint64_t region_id;
  uint64_t size;
};

struct CtrlMsg {
  uint64_t id;
  CtrlOpCode op;
  CtrlRetCode ret;
  MemMsg mmsg;
};

static inline const std::string get_sendq_name(uint64_t id) {
  return "sendq-" + std::to_string(id);
}

static inline const std::string get_recvq_name(uint64_t id) {
  return "recvq-" + std::to_string(id);
}

static inline const std::string get_region_name(uint64_t pid, uint64_t rid) {
  return "region-" + std::to_string(pid) + "-" + std::to_string(rid);
}
}
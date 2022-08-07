#pragma once

#include <cstdint>
#include <sys/types.h>

constexpr static uint32_t kDaemonQDepth = 1024;
constexpr static uint32_t kClientQDepth = 128;
constexpr static char kNameCtrlQ[] = "daemon_ctrl_mq";

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

struct MemMsg {
  uint64_t region_id;
  uint64_t size;
};

struct CtrlMsg {
  pid_t pid;
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
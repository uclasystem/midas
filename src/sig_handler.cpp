#include <bits/types/siginfo_t.h>
#include <cassert>
#include <csignal>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <sys/ucontext.h>
#include <ucontext.h>

#include "logging.hpp"
#include "sig_handler.hpp"

extern "C" void print_callstack(siginfo_t *info, ucontext_t *ctx);

namespace cachebank {
void SigHandler::init() {
  // register rmemcpy
  register_func(reinterpret_cast<uint64_t>(&rmemcpy),
                reinterpret_cast<uint64_t>(&rmemcpy_end));
}

void SigHandler::register_func(uint64_t stt_ip, uint64_t end_ip) {
  assert(stt_ip <= end_ip);
  funcs.emplace_back(stt_ip, end_ip);
}

ResilientFunc *SigHandler::dispatcher(uint64_t ip) {
  ResilientFunc *ret = nullptr;
  for (auto &f : funcs) {
    if (f.contain(ip)) {
      ret = &f;
      break;
    }
  }
  return ret;
}

bool SigHandler::softfault_handler(siginfo_t *info, ucontext_t *ctx) {
  if (!in_volatile_range((uint64_t)info->si_addr))
    return false;

  void *ip = (void *)ctx->uc_mcontext.gregs[REG_RIP];
  uint64_t *bp = (uint64_t *)ctx->uc_mcontext.gregs[REG_RBP];
  LOG_PRINTF(kError, "ip = %p, bp = %p\n", ip, bp);

  auto func = dispatcher(ctx->uc_mcontext.gregs[REG_RIP]);
  if (!func)
    return false;

  // fault handling
  if (func->omitted_frame_pointer) { // jump to ret directly
    ctx->uc_mcontext.gregs[REG_RIP] = func->fail_entry;
    ctx->uc_mcontext.gregs[REG_RAX] = 0; // return value
  } else { // return to the upper level stack
    ctx->uc_mcontext.gregs[REG_RIP] = bp[1];
    ctx->uc_mcontext.gregs[REG_RBP] = bp[0];
    ctx->uc_mcontext.gregs[REG_RSP] = reinterpret_cast<uint64_t>(bp);
    ctx->uc_mcontext.gregs[REG_RAX] = 0; // return value
  }

  LOG_PRINTF(kError, "return to ip = %p, rbp = %p, rsp = %p\n",
             (void *)ctx->uc_mcontext.gregs[REG_RIP],
             (void *)ctx->uc_mcontext.gregs[REG_RBP],
             (void *)ctx->uc_mcontext.gregs[REG_RSP]);
  return true;
}

static inline bool softfault_handler(siginfo_t *info, ucontext_t *ptr) {
  return SigHandler::global_sighandler()->softfault_handler(info, ptr);
}

static void signal_segv(int signum, siginfo_t *info, void *ptr) {
  ucontext_t *ctx = reinterpret_cast<ucontext_t *>(ptr);
  printf("Segmentation Fault!\n");
  // print_callstack(info, ctx);
  if (softfault_handler(info, ctx))
    return;
  exit(-1);
}

void setup_sigsegv() {
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_sigaction = signal_segv;
  action.sa_flags = SA_SIGINFO;
  if (sigaction(SIGSEGV, &action, NULL) < 0)
    LOG(kError) << "sigaction failed!";
}

SigHandler::SigHandler() { setup_sigsegv(); }

// utils
// YIFAN: a very naive copy implementation
bool __attribute__((noinline)) rmemcpy(void *dst, const void *src, size_t len) {
  for (int i = 0; i < len; i++) {
    reinterpret_cast<char *>(dst)[i] = reinterpret_cast<const char *>(src)[i];
  }
  return true;
}
void rmemcpy_end() { asm volatile(".byte 0xcc, 0xcc, 0xcc, 0xcc"); }

} // namespace cachebank
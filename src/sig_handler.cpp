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

namespace midas {
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
  MIDAS_LOG_PRINTF(kError, "fault @ %p, ip = %p, bp = %p\n", info->si_addr, ip,
                   bp);
  // return false;

  auto func = dispatcher(ctx->uc_mcontext.gregs[REG_RIP]);
  if (!func)
    return false;

  // fault handling
  if (func->omitted_frame_pointer) { // jump to ret directly
    ctx->uc_mcontext.gregs[REG_RIP] = func->fail_entry;
    ctx->uc_mcontext.gregs[REG_RAX] = 0; // return value
  } else {                               // return to the upper level stack
    ctx->uc_mcontext.gregs[REG_RIP] = bp[1];
    ctx->uc_mcontext.gregs[REG_RBP] = bp[0];
    ctx->uc_mcontext.gregs[REG_RSP] = reinterpret_cast<uint64_t>(bp);
    ctx->uc_mcontext.gregs[REG_RAX] = 0; // return value
  }

  MIDAS_LOG_PRINTF(kDebug, "return to ip = %p, rbp = %p, rsp = %p\n",
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
  MIDAS_LOG(kDebug) << "Segmentation Fault!";
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
    MIDAS_LOG(kError) << "sigaction failed!";
}

SigHandler::SigHandler() { setup_sigsegv(); }

} // namespace midas
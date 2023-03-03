#pragma once

#include <cstdint>
#include <vector>
extern "C" {
#include <signal.h>
#include <sys/ucontext.h>
}

#include "utils.hpp"
#include "resilient_func.hpp"

namespace midas {

class SigHandler {
public:
  SigHandler();
  void init();
  void register_func(uint64_t stt_ip, uint64_t end_ip);
  bool softfault_handler(siginfo_t *info, ucontext_t *ctx);

  static SigHandler *global_sighandler();

private:
  ResilientFunc *dispatcher(uint64_t ip);

  std::vector<ResilientFunc> funcs;
};
} // namespace midas

/** Implemented in stacktrace.cpp */
extern "C" void print_callstack(siginfo_t *info, ucontext_t *ctx);

#include "impl/sig_handler.ipp"
#include <cstdint>
#include <sys/ucontext.h>
#include <vector>
extern "C" {
#include <signal.h>
}

#include "utils.hpp"

namespace cachebank {

class ResilientFunc {
public:
  ResilientFunc(uint64_t stt_ip_, uint64_t end_ip_);
  void init(void *func_addr);
  bool contain(uint64_t fault_ip);
  bool omitted_frame_pointer;
  uint64_t fail_entry;

private:
  uint64_t stt_ip;
  uint64_t end_ip;
};

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

bool FORCE_INLINE rmemcpy(void *dst, const void *src,
                          size_t len) SOFT_RESILIENT;
void FORCE_INLINE rmemcpy_end() SOFT_RESILIENT;
} // namespace cachebank

/** Implemented in stacktrace.cpp */
extern "C" void print_callstack(siginfo_t *info, ucontext_t *ctx);

#include "impl/sig_handler.ipp"
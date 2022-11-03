#include <cstdint>
#include <sys/ucontext.h>
#include <vector>
extern "C" {
#include <signal.h>
}

namespace cachebank {

class ResilientFunc {
public:
  ResilientFunc(uint64_t stt_ip_, uint64_t end_ip_, uint64_t fail_entry_);
  void init(void *func_addr);
  bool contain(uint64_t fault_ip);
  uint64_t fail_entry;

private:
  uint64_t stt_ip;
  uint64_t end_ip;
};

class SigHandler {
public:
  SigHandler();
  void init();
  void register_func(uint64_t stt_ip, uint64_t end_ip, uint64_t fail_entry);
  bool softfault_handler(siginfo_t *info, ucontext_t *ctx);

  static SigHandler *global_sighandler();

private:
  ResilientFunc *dispatcher(uint64_t ip);

  std::vector<ResilientFunc> funcs;
};

bool __attribute__((noinline)) rmemcpy(void *dst, const void *src, size_t len);
void rmemcpy_end();

} // namespace cachebank

extern "C" void print_callstack(siginfo_t *info, ucontext_t *ctx);

#include "impl/sig_handler.ipp"
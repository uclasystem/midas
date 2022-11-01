#if !defined(__cplusplus) && !defined(NO_CPP_DEMANGLE)
#define NO_CPP_DEMANGLE
#endif

#include <csetjmp>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>

#include <dlfcn.h>
#include <ucontext.h>
#include <unistd.h>
#ifndef NO_CPP_DEMANGLE
#include <cxxabi.h>
#ifdef __cplusplus
using __cxxabiv1::__cxa_demangle;
#endif
#endif

#include "../inc/utils.hpp"

void *copy_from_stt_addr = nullptr;
void *copy_from_end_addr = nullptr;
void *copy_to_stt_addr = nullptr;
void *copy_to_end_addr = nullptr;

bool in_volatile_range(uint64_t addr) {
  return addr >= cachebank::kVolatileSttAddr &&
         addr < cachebank::kVolatileEndAddr;
}

static void signal_segv(int signum, siginfo_t *info, void *ptr) {
  static const char *si_codes[3] = {"", "SEGV_MAPERR", "SEGV_ACCERR"};

  ucontext_t *ucontext = (ucontext_t *)ptr;
  Dl_info dlinfo;
  int f = 0;
  void **bp = nullptr;
  void *ip = nullptr;

  std::cout << "Function addr: " << copy_from_stt_addr << " "
            << copy_from_end_addr << std::endl;

  std::cout << "Segmentation Fault!" << std::endl;

  if (!in_volatile_range((uint64_t)info->si_addr))
    exit(-1);

  printf("info.si_signo = %d\n", signum);
  printf("info.si_errno = %d\n", info->si_errno);
  printf("info.si_code  = %d (%s)\n", info->si_code, si_codes[info->si_code]);
  printf("info.si_addr  = %p\n", info->si_addr);
  // for (int i = 0; i < NGREG; i++)
  //   printf("reg[%02d]       = 0x%llx\n", i, ucontext->uc_mcontext.gregs[i]);

#define SIGSEGV_STACK_IA64
#ifndef SIGSEGV_NOSTACK
#if defined(SIGSEGV_STACK_IA64) || defined(SIGSEGV_STACK_X86)
#if defined(SIGSEGV_STACK_IA64)
  ip = (void *)ucontext->uc_mcontext.gregs[REG_RIP];
  bp = (void **)ucontext->uc_mcontext.gregs[REG_RBP];
#elif defined(SIGSEGV_STACK_X86)
  ip = (void *)ucontext->uc_mcontext.gregs[REG_EIP];
  bp = (void **)ucontext->uc_mcontext.gregs[REG_EBP];
#endif

  std::cout << "ip: " << ip << ", bp: " << bp << std::endl;
  // fault handling: return to the upper level stack
  bool stack_opt = false;
  if (stack_opt) { // YIFAN: doesn't work for now
    ip = bp[1];
    int eax = 0;
    ucontext->uc_mcontext.gregs[REG_RIP] = (int64_t)ip;
    ucontext->uc_mcontext.gregs[REG_RSP] =
        ucontext->uc_mcontext.gregs[REG_RSP] + 8;
    ucontext->uc_mcontext.gregs[REG_RAX] = eax;
  } else {
    ip = bp[1];
    bp = (void **)bp[0];
    int eax = 0;
    ucontext->uc_mcontext.gregs[REG_RIP] = (int64_t)ip;
    ucontext->uc_mcontext.gregs[REG_RSP] = ucontext->uc_mcontext.gregs[REG_RBP];
    ucontext->uc_mcontext.gregs[REG_RBP] = (int64_t)bp;
    ucontext->uc_mcontext.gregs[REG_RAX] = eax;
  }

  return;

  printf("Stack trace:\n");
  while (bp && ip) {
    if (!dladdr(ip, &dlinfo))
      break;

    const char *symname = dlinfo.dli_sname;

#ifndef NO_CPP_DEMANGLE
    int status;
    char *tmp = __cxa_demangle(symname, NULL, 0, &status);

    if (status == 0 && tmp)
      symname = tmp;
#endif

    printf("% 2d: %p <%s+%lu> (%s)\n", ++f, ip, symname,
           (unsigned long)ip - (unsigned long)dlinfo.dli_saddr,
           dlinfo.dli_fname);

#ifndef NO_CPP_DEMANGLE
    if (tmp)
      free(tmp);
#endif

    if (dlinfo.dli_sname && !strcmp(dlinfo.dli_sname, "main"))
      break;

    ip = bp[1];
    bp = (void **)bp[0];
  }
#else
  printf("Stack trace (non-dedicated):\n");
  sz = backtrace(bt, 20);
  strings = backtrace_symbols(bt, sz);
  for (int i = 0; i < sz; ++i)
    printf("%s\n", strings[i]);
#endif
  printf("End of stack trace.\n");
#else
  printf("Not printing stack strace.\n");
#endif
  exit(-1);
}

static void setup_sigsegv() {
  struct sigaction action;
  memset(&action, 0, sizeof(action));
  action.sa_sigaction = signal_segv;
  action.sa_flags = SA_SIGINFO;
  if (sigaction(SIGSEGV, &action, NULL) < 0)
    perror("sigaction");
}

class SoftPtr {
public:
  SoftPtr(bool valid = true) {
    if (valid) {
      ptr_ = new int[10];
    } else {
      // ptr_ = nullptr;
      ptr_ = reinterpret_cast<int *>(kInvPtr);
    }
    std::cout << "SoftPtr(): ptr_ = " << ptr_ << std::endl;
  }
  ~SoftPtr() {
    std::cout << "~SoftPtr(): ptr_ = " << ptr_ << std::endl;
    if (reinterpret_cast<int64_t>(ptr_) != kInvPtr && ptr_ != nullptr)
      delete[] ptr_;
  }

  void reset() { ptr_ = nullptr; }

  bool __attribute__((optimize(0)))
  copy_from(void *src, size_t size, int64_t offset = 0);
  bool __attribute__((optimize(0)))
  copy_to(void *dst, size_t size, int64_t offset = 0);
  // bool copy_from(void *src, size_t size, int64_t offset = 0);
  // bool copy_to(void *dst, size_t size, int64_t offset = 0);

private:
  constexpr static uint64_t kInvPtr = cachebank::kVolatileSttAddr + 0x100200300;
  int *ptr_;
};

bool SoftPtr::copy_from(void *src, size_t size, int64_t offset) {
start:
  if (UNLIKELY(!copy_from_stt_addr))
    copy_from_stt_addr = &&start;
  if (UNLIKELY(!copy_from_end_addr))
    copy_from_end_addr = &&end;
  std::memcpy(ptr_, src, size);

end:
  return true;
}

bool SoftPtr::copy_to(void *dst, size_t size, int64_t offset) {
start:
  if (UNLIKELY(!copy_to_stt_addr))
    copy_to_stt_addr = &&start;
  if (UNLIKELY(!copy_to_end_addr))
    copy_to_end_addr = &&end;
  std::memcpy(dst, ptr_, size);

end:
  return true;
}

int main() {
  setup_sigsegv();

  SoftPtr sptr(false);
  int a = 10;
  bool ret = sptr.copy_from(&a, sizeof(int));
  std::cout << "ret " << ret << std::endl;
  if (!ret)
    sptr.reset();
  std::cout << "Function addr: "
            // << reinterpret_cast<void *>(&SoftPtr::copy_from) << " "
            << copy_from_stt_addr << " " << copy_from_end_addr << std::endl;

  std::cout << "Test passed!" << std::endl;

  return 0;
}
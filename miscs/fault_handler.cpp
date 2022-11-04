#include <bits/types/siginfo_t.h>
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

// YIFAN: very naive copy
bool __attribute__((noinline)) rmemcpy(void *dst, void *src, size_t len) {
  for (int i = 0; i < len; i++) {
    reinterpret_cast<char *>(dst)[i] = reinterpret_cast<char *>(src)[i];
  }
  return true;
}

void rmemcpy_end() {
  asm volatile (".byte 0xcc, 0xcc, 0xcc");
}

void *rmemcpy_stt_addr = reinterpret_cast<void *>(&rmemcpy);
void *rmemcpy_end_addr = reinterpret_cast<void *>(&rmemcpy_end);

bool in_volatile_range(uint64_t addr) {
  return addr >= cachebank::kVolatileSttAddr &&
         addr < cachebank::kVolatileEndAddr;
}

static bool softfault_handler(siginfo_t *info, ucontext_t *ctx) {
  if (!in_volatile_range((uint64_t)info->si_addr))
    return false;

  void *ip = (void *)ctx->uc_mcontext.gregs[REG_RIP];
  void **bp = (void **)ctx->uc_mcontext.gregs[REG_RBP];
  printf("ip = %p,\tbp = %p\n", ip, bp);

  // fault handling: return to the upper level stack
  bool stack_opt = false;
  if (stack_opt) { // YIFAN: doesn't work for now
    int eax = 0;
    ctx->uc_mcontext.gregs[REG_RIP] = (int64_t)bp[1];
    ctx->uc_mcontext.gregs[REG_RSP] = ctx->uc_mcontext.gregs[REG_RSP] + 8;
    ctx->uc_mcontext.gregs[REG_RAX] = eax;
  } else {
    printf("bp[0,1] = %p %p\n", bp[0], bp[1]);
    // ctx->uc_mcontext.gregs[REG_RIP] = (int64_t)rmemcpy_end_addr - 2;
    ctx->uc_mcontext.gregs[REG_RIP] = (int64_t)bp[1];
    ctx->uc_mcontext.gregs[REG_RBP] = (int64_t)bp[0];
    ctx->uc_mcontext.gregs[REG_RSP] = (int64_t)bp;
    ctx->uc_mcontext.gregs[REG_RAX] = 0; // return value
    printf("return to ip = %p, rbp = %p, rsp = %p\n",
           (void *)ctx->uc_mcontext.gregs[REG_RIP],
           (void *)ctx->uc_mcontext.gregs[REG_RBP],
           (void *)ctx->uc_mcontext.gregs[REG_RSP]);
  }
  return true;
}

static void print_callstack(siginfo_t *info, ucontext_t *ctx) {
  static const char *si_codes[3] = {"", "SEGV_MAPERR", "SEGV_ACCERR"};

  printf("info.si_signo = %d\n", info->si_signo);
  printf("info.si_errno = %d\n", info->si_errno);
  printf("info.si_code  = %d (%s)\n", info->si_code, si_codes[info->si_code]);
  printf("info.si_addr  = %p\n", info->si_addr);
  // for (int i = 0; i < NGREG; i++)
  //   printf("reg[%02d]       = 0x%llx\n", i, ucontext->uc_mcontext.gregs[i]);

  int f = 0;
  Dl_info dlinfo;
  void *ip = (void *)ctx->uc_mcontext.gregs[REG_RIP];
  void **bp = (void **)ctx->uc_mcontext.gregs[REG_RBP];
  printf("ip = %p,\tbp = %p\n", ip, bp);

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
}

static void signal_segv(int signum, siginfo_t *info, void *ptr) {
  ucontext_t *ctx = (ucontext_t *)ptr;

  printf("Segmentation Fault!\n");

  // print_callstack(info, ctx);
  if (softfault_handler(info, ctx))
    return;
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
    // std::cout << "SoftPtr(): ptr_ = " << ptr_ << std::endl;
  }
  ~SoftPtr() {
    // std::cout << "~SoftPtr(): ptr_ = " << ptr_ << std::endl;
    if (reinterpret_cast<int64_t>(ptr_) != kInvPtr && ptr_)
      delete[] ptr_;
  }

  void reset() { ptr_ = nullptr; }

  bool copy_from(void *src, size_t size, int64_t offset = 0);
  bool copy_to(void *dst, size_t size, int64_t offset = 0);
  // bool copy_from(void *src, size_t size, int64_t offset = 0);
  // bool copy_to(void *dst, size_t size, int64_t offset = 0);

private:
  constexpr static uint64_t kInvPtr = cachebank::kVolatileSttAddr + 0x100200300;
  int *ptr_;
};

bool SoftPtr::copy_from(void *src, size_t size, int64_t offset) {
  return memcpy(ptr_, src, size);
}

bool SoftPtr::copy_to(void *dst, size_t size, int64_t offset) {
  return memcpy(dst, ptr_, size);
}

void do_work() {
  SoftPtr sptr(false);
  int a = 10;
  bool ret = sptr.copy_from(&a, sizeof(int));
  std::cout << "copy_from returns " << ret << std::endl;
  ret = sptr.copy_to(&a, sizeof(int));
  std::cout << "copy_to   returns " << ret << std::endl;
  if (!ret)
    sptr.reset();
}

int main() {
  setup_sigsegv();
  std::cout << "memcpy() function range: " << rmemcpy_stt_addr << " "
            << rmemcpy_end_addr << std::endl;
  do_work();
  std::cout << "Test passed!" << std::endl;

  return 0;
}
extern "C" {
#include <setjmp.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <dlfcn.h>
#include <ucontext.h>
#include <unistd.h>
#ifndef NO_CPP_DEMANGLE
#include <cxxabi.h>
#ifdef __cplusplus
using __cxxabiv1::__cxa_demangle;
#endif
#endif

void print_callstack(siginfo_t *info, ucontext_t *ctx) {
  const bool verbose = false;
  static const char *si_codes[3] = {"", "SEGV_MAPERR", "SEGV_ACCERR"};

  printf("info.si_signo = %d\n", info->si_signo);
  printf("info.si_errno = %d\n", info->si_errno);
  printf("info.si_code  = %d (%s)\n", info->si_code, si_codes[info->si_code]);
  printf("info.si_addr  = %p\n", info->si_addr);
  if (verbose)
    for (int i = 0; i < NGREG; i++)
      printf("reg[%02d]       = 0x%llx\n", i, ctx->uc_mcontext.gregs[i]);

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
}
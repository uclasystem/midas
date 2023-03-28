#include "page_cache.hpp"

#ifdef HIJACK_FS_SYSCALLS

extern "C" {
namespace midas {
int open(const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->open != NULL);

  int fd;
  if ((fd = pc_intor->open(pathname, flags, mode)) != -1) {
    MIDAS_LOG_PRINTF(kDebug, "open(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                     pathname, flags, mode, fd);
  }
  return fd;
}

int open64(const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->open64 != NULL);

  MIDAS_LOG_PRINTF(kDebug, "open64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = pc_intor->open64(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int creat(const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->creat != NULL);

  MIDAS_LOG_PRINTF(kDebug, "creat(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = pc_intor->creat(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int creat64(const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->creat64 != NULL);

  MIDAS_LOG_PRINTF(kDebug, "creat64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = pc_intor->creat64(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int openat(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->openat != NULL);

  MIDAS_LOG_PRINTF(kDebug,
                   "openat(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  int fd;
  if ((fd = pc_intor->openat(dirfd, pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->openat64 != NULL);

  MIDAS_LOG_PRINTF(kDebug,
                   "openat64(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  int fd;
  if ((fd = pc_intor->openat64(dirfd, pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int dup(int oldfd) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->dup != NULL);

  MIDAS_LOG_PRINTF(kDebug, "dup(oldfd=%d)\n", oldfd);
  int fd;
  if ((fd = pc_intor->dup(oldfd)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int dup2(int oldfd, int newfd) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->dup2 != NULL);
  int ret;

  /* if newfd is already opened, the kernel will close it directly
   * once dup2 is invoked. So now is the last chance to mark the
   * pages as "DONTNEED" */
  // if (valid_fd(newfd))
  //   free_unclaimed_pages(newfd, true);

  MIDAS_LOG_PRINTF(kDebug, "dup2(oldfd=%d, newfd=%d)\n", oldfd, newfd);
  if ((ret = pc_intor->dup2(oldfd, newfd)) != -1) {
    // store_pageinfo(newfd);
  }
  return ret;
}

int close(int fd) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->close != NULL);

  // free_unclaimed_pages(fd, true);

  MIDAS_LOG_PRINTF(kDebug, "close(%d)\n", fd);
  return pc_intor->close(fd);
}

FILE *fopen(const char *path, const char *mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->open != NULL);

  int fd;
  FILE *fp = NULL;

  MIDAS_LOG_PRINTF(kDebug, "fopen(path=%s, mode=%s)\n", path, mode);

  if ((fp = pc_intor->fopen(path, mode)) != NULL) {
    if ((fd = fileno(fp)) != -1) {
      // store_pageinfo(fd);
    }
  }

  return fp;
}

FILE *fopen64(const char *path, const char *mode) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->open != NULL);

  int fd;
  FILE *fp;
  fp = NULL;

  MIDAS_LOG_PRINTF(kDebug, "fopen64(path=%s, mode=%s)\n", path, mode);

  if ((fp = pc_intor->fopen64(path, mode)) != NULL) {
    if ((fd = fileno(fp)) != -1) {
      // store_pageinfo(fd);
    }
  }

  return fp;
}

int fclose(FILE *fp) {
  auto pc_intor = PageCacheInterceptor::global_interceptor();
  assert(pc_intor->open != NULL);

  if (pc_intor->fclose) {
    // free_unclaimed_pages(fileno(fp), true);
    return pc_intor->fclose(fp);
  }

  errno = EFAULT;
  return EOF;
}
} // namespace midas
}

#endif // HIJACK_FS_SYSCALLS
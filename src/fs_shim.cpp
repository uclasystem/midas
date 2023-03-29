#include "fs_shim.hpp"
#include "logging.hpp"

#ifdef HIJACK_FS_SYSCALLS

extern "C" {
namespace midas {
int open(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open != NULL);

  int fd;
  if ((fd = shim->open(pathname, flags, mode)) != -1) {
    MIDAS_LOG_PRINTF(kDebug, "open(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
                     pathname, flags, mode, fd);
  }
  return fd;
}

int open64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open64 != NULL);

  MIDAS_LOG_PRINTF(kDebug, "open64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = shim->open64(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int creat(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat != NULL);

  MIDAS_LOG_PRINTF(kDebug, "creat(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = shim->creat(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int creat64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat64 != NULL);

  MIDAS_LOG_PRINTF(kDebug, "creat64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd;
  if ((fd = shim->creat64(pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int openat(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat != NULL);

  MIDAS_LOG_PRINTF(kDebug,
                   "openat(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  int fd;
  if ((fd = shim->openat(dirfd, pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat64 != NULL);

  MIDAS_LOG_PRINTF(kDebug,
                   "openat64(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  int fd;
  if ((fd = shim->openat64(dirfd, pathname, flags, mode)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int dup(int oldfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup != NULL);

  MIDAS_LOG_PRINTF(kDebug, "dup(oldfd=%d)\n", oldfd);
  int fd;
  if ((fd = shim->dup(oldfd)) != -1) {
    // store_pageinfo(fd);
  }
  return fd;
}

int dup2(int oldfd, int newfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup2 != NULL);
  int ret;

  /* if newfd is already opened, the kernel will close it directly
   * once dup2 is invoked. So now is the last chance to mark the
   * pages as "DONTNEED" */
  // if (valid_fd(newfd))
  //   free_unclaimed_pages(newfd, true);

  MIDAS_LOG_PRINTF(kDebug, "dup2(oldfd=%d, newfd=%d)\n", oldfd, newfd);
  if ((ret = shim->dup2(oldfd, newfd)) != -1) {
    // store_pageinfo(newfd);
  }
  return ret;
}

int close(int fd) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  // free_unclaimed_pages(fd, true);

  MIDAS_LOG_PRINTF(kDebug, "close(%d)\n", fd);
  return shim->close(fd);
}

FILE *fopen(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen != NULL);

  int fd;
  FILE *fp = NULL;

  MIDAS_LOG_PRINTF(kDebug, "fopen(path=%s, mode=%s)\n", path, mode);

  if ((fp = shim->fopen(path, mode)) != NULL) {
    if ((fd = fileno(fp)) != -1) {
      // store_pageinfo(fd);
    }
  }

  return fp;
}

FILE *fopen64(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen64 != NULL);

  int fd;
  FILE *fp;
  fp = NULL;

  MIDAS_LOG_PRINTF(kDebug, "fopen64(path=%s, mode=%s)\n", path, mode);

  if ((fp = shim->fopen64(path, mode)) != NULL) {
    if ((fd = fileno(fp)) != -1) {
      // store_pageinfo(fd);
    }
  }

  return fp;
}

int fclose(FILE *fp) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  if (shim->fclose) {
    // free_unclaimed_pages(fileno(fp), true);
    return shim->fclose(fp);
  }

  errno = EFAULT;
  return EOF;
}

ssize_t read(int fd, void *buf, size_t count) {
  auto shim = FSShim::global_shim();
  assert(shim->read != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture read %p %lu\n", buf, count);
  return shim->read(fd, buf, count);
}

ssize_t write(int fd, const void *buf, size_t count) {
  auto shim = FSShim::global_shim();
  assert(shim->write != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture write %p %lu\n", buf, count);
  return shim->write(fd, buf, count);
}

ssize_t pread(int fd, void *buf, size_t count, off_t offset) {
  auto shim = FSShim::global_shim();
  assert(shim->pread != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture pread %p %lu\n", buf, count);
  return shim->pread(fd, buf, count, offset);
}

ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset) {
  auto shim = FSShim::global_shim();
  assert(shim->pwrite != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture pwrite %p %lu\n", buf, count);
  return shim->pwrite(fd, buf, count, offset);
}

size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
  auto shim = FSShim::global_shim();
  assert(shim->fread != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture fread\n");
  return shim->fread(ptr, size, nmemb, stream);
}

size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream) {
  auto shim = FSShim::global_shim();
  assert(shim->fwrite != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture fwrite\n");
  return shim->fwrite(ptr, size, nmemb, stream);
}

off_t lseek(int fd, off_t offset, int whence) {
  auto shim = FSShim::global_shim();
  assert(shim->lseek != NULL);
  MIDAS_LOG_PRINTF(kDebug, "capture lseek\n");
  return shim->lseek(fd, offset, whence);
}

} // namespace midas
}

#endif // HIJACK_FS_SYSCALLS
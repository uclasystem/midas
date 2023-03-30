#include "fs_shim.hpp"
#include "logging.hpp"

#ifdef HIJACK_FS_SYSCALLS

extern "C" {
namespace midas {
/** Intercept fs-related syscalls */
int open(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open != NULL);

  int fd = shim->open(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname))
    return fd;
  MIDAS_LOG_PRINTF(kDebug, "open(pathname=%s, flags=0x%x, mode=0%o) = %d\n",
  return fd;
}

int open64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->open64 != NULL);

  int fd = shim->open64(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname))
    return fd;
  MIDAS_LOG_PRINTF(kDebug, "open64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  return fd;
}

int creat(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat != NULL);

  int fd = shim->creat(pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname))
    return fd;
  MIDAS_LOG_PRINTF(kDebug, "creat(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  return fd;
}

int creat64(const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->creat64 != NULL);

  MIDAS_LOG_PRINTF(kDebug, "creat64(pathname=%s, flags=0x%x, mode=0%o)\n",
                   pathname, flags, mode);
  int fd = shim->creat64(pathname, flags, mode);
  assert(fd != -1);
  return fd;
}

int openat(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat != NULL);

  int fd = shim->openat(dirfd, pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname))
    return fd;
  MIDAS_LOG_PRINTF(kDebug,
                   "openat(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  return fd;
}

int openat64(int dirfd, const char *pathname, int flags, mode_t mode) {
  auto shim = FSShim::global_shim();
  assert(shim->openat64 != NULL);

  int fd = shim->openat64(dirfd, pathname, flags, mode);
  if (fd == -1)
    return fd;
  if (is_shm_file(pathname))
    return fd;
  MIDAS_LOG_PRINTF(kDebug,
                   "openat64(dirfd=%d, pathname=%s, flags=0x%x, mode=0%o)\n",
                   dirfd, pathname, flags, mode);
  return fd;
}

int dup(int oldfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup != NULL);

  int fd = shim->dup(oldfd);
  if (fd == -1)
    return fd;
  MIDAS_LOG_PRINTF(kDebug, "dup(oldfd=%d)\n", oldfd);
  return fd;
}

int dup2(int oldfd, int newfd) {
  auto shim = FSShim::global_shim();
  assert(shim->dup2 != NULL);

  /* if newfd is already opened, the kernel will close it directly
   * once dup2 is invoked. So now is the last chance to mark the
   * pages as "DONTNEED" */
  // if (valid_fd(newfd))
  //   free_unclaimed_pages(newfd, true);

  int ret = shim->dup2(oldfd, newfd);
  if (ret == -1)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "dup2(oldfd=%d, newfd=%d)\n", oldfd, newfd);
  return ret;
}

int close(int fd) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  int ret = shim->close(fd);
  if (ret == -1)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "close(%d)\n", fd);
  return ret;
}

FILE *fopen(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen != NULL);

  FILE *fp = shim->fopen(path, mode);
  if (fp == nullptr)
    return fp;
  int fd = fileno(fp);
  if (fd == -1)
    return fp;
  if (is_shm_file(path))
    return fp;
  MIDAS_LOG_PRINTF(kDebug, "fopen(path=%s, mode=%s)\n", path, mode);

  return fp;
}

FILE *fopen64(const char *path, const char *mode) {
  auto shim = FSShim::global_shim();
  assert(shim->fopen64 != NULL);

  FILE *fp = shim->fopen64(path, mode);
  if (fp == nullptr)
    return fp;
  int fd = fileno(fp);
  if (fd == -1)
    return fp;
  if (is_shm_file(path))
    return fp;
  MIDAS_LOG_PRINTF(kDebug, "fopen64(path=%s, mode=%s)\n", path, mode);

  return fp;
}

int fclose(FILE *fp) {
  auto shim = FSShim::global_shim();
  assert(shim->close != NULL);

  int ret = shim->fclose(fp);
  if (ret != 0)
    return ret;
  MIDAS_LOG_PRINTF(kDebug, "fclose(fp=%p)\n", fp);

  return ret;
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
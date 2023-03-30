#pragma once

// #define HIJACK_FS_SYSCALLS
#ifdef HIJACK_FS_SYSCALLS

extern "C" {
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>

/** NOTE: commented out below to avoid declaration conflicts with fcntl.h
 *    This is okay as long as fcntl.h is included, which is done by our
 * shared-memory-based IPC implementation.
 */
/** File open/close related syscalls */
// int open(const char *pathname, int flags, mode_t mode);
// int open64(const char *pathname, int flags, mode_t mode);
// int creat(const char *pathname, int flags, mode_t mode);
// int creat64(const char *pathname, int flags, mode_t mode);
// int openat(int dirfd, const char *pathname, int flags, mode_t mode);
// int openat64(int dirfd, const char *pathname, int flags, mode_t mode);
// int __openat_2(int dirfd, const char *pathname, int flags, mode_t mode)
//     __attribute__((alias("openat")));
// int dup(int oldfd);
// int dup2(int oldfd, int newfd);
// int close(int fd);
// FILE *fopen(const char *path, const char *mode);
// FILE *fopen64(const char *path, const char *mode);
// int fclose(FILE *fp);
// /** File read/write related syscalls */
// ssize_t read(int fd, void *buf, size_t count);
// ssize_t write(int fd, const void *buf, size_t count);
// ssize_t pread(int fd, void *buf, size_t count, off_t offset);
// ssize_t pwrite(int fd, const void *buf, size_t count, off_t offset);
// size_t fread(void *ptr, size_t size, size_t nmemb, FILE *stream);
// size_t fwrite(const void *ptr, size_t size, size_t nmemb, FILE *stream);
// off_t lseek(int fd, off_t offset, int whence);
}

#include <memory>
#include <mutex>
#include <unordered_map>
#include <set>

#include "logging.hpp"
#include "sync_hashmap.hpp"
#include "utils.hpp"

namespace midas {
using pfn_t = int64_t; // page frame number
using Page = char[kPageSize];
constexpr static int32_t kNumPageCacheBuckets = 1 << 4;
using PageMap = SyncHashMap<kNumPageCacheBuckets, pfn_t, Page>;

struct File {
  File() = default;
  File(CachePool *pool);

  off_t offset;
  std::shared_ptr<PageMap> cache;
};

class FSShim {
public:
  FSShim();

  bool on_open(int fd);
  bool on_close(int fd);
  ssize_t on_read(int fd, void *buf, size_t count, bool upd_offset,
                  off_t offset = 0);
  ssize_t on_write(int fd, const void *buf, size_t count, bool upd_offset,
                   off_t offset = 0);
  bool on_lseek(int fd, off_t offset);

  // As we use shm files for IPC now, we need to exclude them from interposition
  void exclude_interpose(int fd);
  void reset_interpose(int fd);
  bool is_excluded(int fd);

  static inline FSShim *global_shim();

  // origial syscalls
  int (*open)(const char *pathname, int flags, mode_t mode);
  int (*open64)(const char *pathname, int flags, mode_t mode);
  int (*creat)(const char *pathname, int flags, mode_t mode);
  int (*creat64)(const char *pathname, int flags, mode_t mode);
  int (*openat)(int dirfd, const char *pathname, int flags, mode_t mode);
  int (*openat64)(int dirfd, const char *pathname, int flags, mode_t mode);
  int (*dup)(int fd);
  int (*dup2)(int newfd, int oldfd);
  int (*close)(int fd);
  FILE *(*fopen)(const char *path, const char *mode);
  FILE *(*fopen64)(const char *path, const char *mode);
  int (*fclose)(FILE *fp);

  ssize_t (*read)(int fd, void *buf, size_t count);
  ssize_t (*write)(int fd, const void *buf, size_t count);
  ssize_t (*pread)(int fd, void *buf, size_t count, off_t offset);
  ssize_t (*pwrite)(int fd, const void *buf, size_t count, off_t offset);
  size_t (*fread)(void *ptr, size_t size, size_t nmemb, FILE *stream);
  size_t (*fwrite)(const void *ptr, size_t size, size_t nmemb, FILE *stream);
  off_t (*lseek)(int fd, off_t offset, int whence);

private:
  void capture_syscalls();
  void init_cache();

  std::mutex mtx_;
  std::unordered_map<ino_t, File> files_; // file's inode number to its cache
  std::mutex bl_mtx_;
  std::set<int> black_list_; // we need to exclude shm files from interposition
  CachePool *pool_;
  constexpr static char pool_name[] = "page-cache";
  constexpr static int64_t kPageCacheLimit = 100ul * 1024 * 1024; // 100 MB
};

static inline ino_t get_inode(int fd);
static inline bool invalid_inode(ino_t inode);
static inline bool is_shm_file(const char *pathname);
} // namespace midas

#include "impl/fs_shim.ipp"

#endif // HIJACK_FS_SYSCALLS
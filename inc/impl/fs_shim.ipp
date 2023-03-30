#pragma once

namespace midas {
inline File::File(CachePool *pool)
    : offset(0), cache(std::make_shared<PageMap>(pool)) {}

inline FSShim::FSShim() { capture_syscalls(); }

inline void FSShim::init_cache() {
  if (pool_)
    return;
  auto cmanager = CacheManager::global_cache_manager();
  assert(cmanager->create_pool(pool_name));
  pool_ = cmanager->get_pool(pool_name);
  assert(pool_);
  pool_->update_limit(kPageCacheLimit);
}

inline void FSShim::capture_syscalls() {
  open = (int (*)(const char *, int, mode_t))dlsym(RTLD_NEXT, "open");
  open64 = (int (*)(const char *, int, mode_t))dlsym(RTLD_NEXT, "open64");
  creat = (int (*)(const char *, int, mode_t))dlsym(RTLD_NEXT, "creat");
  creat64 = (int (*)(const char *, int, mode_t))dlsym(RTLD_NEXT, "creat64");
  openat = (int (*)(int, const char *, int, mode_t))dlsym(RTLD_NEXT, "openat");
  openat64 =
      (int (*)(int, const char *, int, mode_t))dlsym(RTLD_NEXT, "openat64");
  dup = (int (*)(int))dlsym(RTLD_NEXT, "dup");
  dup2 = (int (*)(int, int))dlsym(RTLD_NEXT, "dup2");
  close = (int (*)(int))dlsym(RTLD_NEXT, "close");
  fopen = (FILE * (*)(const char *, const char *)) dlsym(RTLD_NEXT, "fopen");
  fopen64 =
      (FILE * (*)(const char *, const char *)) dlsym(RTLD_NEXT, "fopen64");
  fclose = (int (*)(FILE *))dlsym(RTLD_NEXT, "fclose");

  read = (ssize_t(*)(int, void *, size_t))dlsym(RTLD_NEXT, "read");
  write = (ssize_t(*)(int, const void *, size_t))dlsym(RTLD_NEXT, "write");
  pread = (ssize_t(*)(int, void *, size_t, off_t))dlsym(RTLD_NEXT, "pread");
  pwrite =
      (ssize_t(*)(int, const void *, size_t, off_t))dlsym(RTLD_NEXT, "pwrite");
  fread = (size_t(*)(void *, size_t, size_t, FILE *))dlsym(RTLD_NEXT, "fread");
  fwrite = (size_t(*)(const void *, size_t, size_t, FILE *))dlsym(RTLD_NEXT,
                                                                  "fwrite");
  lseek = (off_t(*)(int, off_t, int))dlsym(RTLD_NEXT, "lseek");

  char *error = nullptr;
  if ((error = dlerror()) != nullptr)
    MIDAS_ABORT("%s", error);
}

inline void FSShim::exclude_interpose(int fd) {
  if (fd <= STDERR_FILENO)
    return;
  std::unique_lock<std::mutex> ul(bl_mtx_);
  black_list_.emplace(fd);
  // MIDAS_LOG(kError) << fd;
}

inline void FSShim::reset_interpose(int fd) {
  if (fd <= STDERR_FILENO)
    return;
  std::unique_lock<std::mutex> ul(bl_mtx_);
  black_list_.erase(fd);
  // MIDAS_LOG(kError) << fd;
}

inline bool FSShim::is_excluded(int fd) {
  if (fd <= STDERR_FILENO)
    return true;
  std::unique_lock<std::mutex> ul(bl_mtx_);
  return black_list_.find(fd) != black_list_.cend();
}

inline FSShim *FSShim::global_shim() {
  static std::mutex mtx_;
  static std::unique_ptr<FSShim> shim_;

  if (shim_)
    return shim_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (shim_)
    return shim_.get();
  shim_ = std::make_unique<FSShim>();
  assert(shim_);
  return shim_.get();
}

constexpr static ino_t INV_INODE = -1;
static inline ino_t get_inode(int fd) {
  struct stat file_stat;
  if (fstat(fd, &file_stat) < 0)
    return INV_INODE;
  return file_stat.st_ino;
}

static inline bool invalid_inode(ino_t inode) { return inode == INV_INODE; }

static inline bool is_shm_file(const char *pathname) {
  constexpr static char shm_prefix[] = "/dev/shm";
  constexpr static int prefix_len = sizeof(shm_prefix) - 1;
  return std::strncmp(pathname, shm_prefix, prefix_len) == 0;
}
} // namespace midas
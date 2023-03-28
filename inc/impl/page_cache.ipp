#pragma once

namespace midas {
PageCacheInterceptor::PageCacheInterceptor() {
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

inline PageCacheInterceptor *PageCacheInterceptor::global_interceptor() {
  static std::mutex mtx_;
  static std::unique_ptr<PageCacheInterceptor> intor_;

  if (intor_)
    return intor_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (intor_)
    return intor_.get();
  intor_ = std::make_unique<PageCacheInterceptor>();
  assert(intor_);
  return intor_.get();
}
} // namespace midas
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

  char *error = nullptr;
  if ((error = dlerror()) != nullptr)
    MIDAS_ABORT("%s", error);
}

inline PageCacheInterceptor *PageCacheInterceptor::global_interceptor() {
  static std::mutex mtx_;
  static std::unique_ptr<PageCacheInterceptor> inter_;

  if (inter_)
    return inter_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (inter_)
    return inter_.get();
  inter_ = std::make_unique<PageCacheInterceptor>();
  assert(inter_);
  return inter_.get();
}
} // namespace midas
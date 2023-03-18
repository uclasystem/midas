#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

namespace midas {

inline Evacuator::Evacuator(CachePool *pool,
                            std::shared_ptr<ResourceManager> rmanager,
                            std::shared_ptr<LogAllocator> allocator)
    : pool_(pool), rmanager_(rmanager), allocator_(allocator),
      terminated_(false) {
  init();
}

inline void Evacuator::signal_gc() {
  std::unique_lock<std::mutex> ul(gc_mtx_);
  gc_cv_.notify_all();
}

inline Evacuator::~Evacuator() {
  terminated_ = true;
  signal_gc();
  if (gc_thd_) {
    gc_thd_->join();
    gc_thd_.reset();
  }
}

} // namespace midas
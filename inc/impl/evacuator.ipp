#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

namespace cachebank {

inline Evacuator::Evacuator() : terminated_(false) {
  init();
}

inline void Evacuator::signal_gc() { gc_cv_.notify_all(); }

inline Evacuator::~Evacuator() {
  terminated_ = true;
  signal_gc();
  if (gc_thd_) {
    gc_thd_->join();
    gc_thd_.reset();
  }
}

inline Evacuator *Evacuator::global_evacuator() {
  static std::mutex mtx_;
  static std::shared_ptr<Evacuator> evac_;
  if (evac_)
    return evac_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (evac_)
    return evac_.get();
  evac_ = std::make_shared<Evacuator>();
  return evac_.get();
}

} // namespace cachebank
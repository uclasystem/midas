#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <thread>

namespace cachebank {

inline Evacuator::Evacuator()
    : nr_gc_thds_(kNumGCThds), under_pressure_(0), terminated_(false) {
  init();
}

inline void Evacuator::signal_scan() { scanner_cv_.notify_all(); }

inline void Evacuator::signal_gc() { evacuator_cv_.notify_all(); }

inline Evacuator::~Evacuator() {
  terminated_ = true;
  if (scanner_thd_) {
    scanner_thd_->join();
    scanner_thd_.reset();
  }
  if (evacuator_thd_) {
    evacuator_thd_->join();
    evacuator_thd_.reset();
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
#pragma once

#include <memory>
#include <mutex>
namespace cachebank {

inline Evacuator::Evacuator() : nr_master_thd(0) { init(); }

inline void Evacuator::init() {}

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
#pragma once

#include <mutex>

namespace cachebank {

inline int64_t Client::new_region_id_() noexcept { return region_cnt_++; }

inline Daemon *Daemon::get_daemon() {
  static std::mutex mtx_;
  static std::shared_ptr<Daemon> daemon_;
  if (daemon_)
    return daemon_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (daemon_)
    return daemon_.get();
  daemon_ = std::make_shared<Daemon>();
  return daemon_.get();
}

} // namespace cachebank
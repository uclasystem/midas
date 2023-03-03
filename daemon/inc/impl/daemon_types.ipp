#pragma once

#include <mutex>

namespace midas {

inline int64_t Client::new_region_id_() noexcept {
  static int64_t region_id = 0;
  return region_id++;
}

inline void Client::alloc_region(size_t size) {
  return alloc_region_(size, false);
}

/* for evacuator to temporarily overcommit memory during evacuation. It will
 * return more regions afterwards. */
inline void Client::overcommit_region(size_t size) {
  return alloc_region_(size, true);
}

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

} // namespace midas
#pragma once

namespace cachebank {

static inline uint64_t get_unique_id() {
  uint64_t pid = boost::interprocess::ipcdetail::get_current_process_id();
  auto creation_time =
      boost::interprocess::ipcdetail::get_current_process_creation_time();

  // TODO: unique id should be the hash of pid and creation_time to avoid pid
  // collision.
  return pid;
}

inline VRange ResourceManager::GetRegion(int64_t region_id) noexcept {
  std::unique_lock<std::mutex> lk(mtx_);
  if (region_map_.find(region_id) == region_map_.cend())
    return VRange();
  auto &region = region_map_[region_id];
  return VRange(region->Addr(), region->Size());
}

inline uint64_t ResourceManager::NumRegionInUse() const noexcept {
  // std::unique_lock<std::mutex> lk(mtx_);
  return region_map_.size();
}

inline uint64_t ResourceManager::NumRegionLimit() const noexcept {
  return region_limit_;
}

/* A thread safe way to create a global manager and get its reference. */
inline ResourceManager *ResourceManager::global_manager() noexcept {
  static std::mutex mtx_;
  static std::unique_ptr<ResourceManager> _rmanager(nullptr);

  if (LIKELY(_rmanager.get() != nullptr))
    return _rmanager.get();

  std::unique_lock<std::mutex> lk(mtx_);
  if (UNLIKELY(_rmanager.get() != nullptr))
    return _rmanager.get();

  _rmanager = std::make_unique<ResourceManager>();
  return _rmanager.get();
}

} // namespace cachebank
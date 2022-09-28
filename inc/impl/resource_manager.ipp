#pragma once

#include <mutex>

namespace cachebank {

static inline uint64_t get_unique_id() {
  auto pid = boost::interprocess::ipcdetail::get_current_process_id();
  auto creation_time =
      boost::interprocess::ipcdetail::get_current_process_creation_time();

  // TODO: unique id should be the hash of pid and creation_time to avoid pid
  // collision.
  return static_cast<uint64_t>(pid);
}

inline VRange ResourceManager::GetRegion(int64_t region_id) noexcept {
  std::unique_lock<std::mutex> lk(_mtx);
  if (_region_map.find(region_id) == _region_map.cend())
    return VRange();
  auto &region = _region_map[region_id];
  return VRange(region->Addr(), region->Size());
}

/* A thread safe way to create a global manager and get its reference. */
inline ResourceManager *ResourceManager::global_manager() noexcept {
  static std::mutex _mtx;
  static std::unique_ptr<ResourceManager> _rmanager(nullptr);

  if (likely(_rmanager.get() != nullptr))
    return _rmanager.get();

  std::unique_lock<std::mutex> lk(_mtx);
  if (unlikely(_rmanager.get() != nullptr))
    return _rmanager.get();

  _rmanager = std::make_unique<ResourceManager>();
  return _rmanager.get();
}

inline SlabAllocator *ResourceManager::global_allocator() noexcept {
  return global_manager()->_allocator.get();
}

} // namespace cachebank
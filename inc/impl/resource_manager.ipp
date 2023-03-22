#pragma once

namespace midas {

static inline uint64_t get_unique_id() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> distr(1, 1ull << 20);

  uint64_t pid = boost::interprocess::ipcdetail::get_current_process_id();
  uint64_t rand = distr(gen);

  return (pid * 1'000'000'000'000ull) + rand;
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
  /* YIFAN: NOTE: so far we don't count regions in freelist as in-use since they
   * are only for reducing IPC across client<-> daemon, and the app never uses
   * free regions until they allocate. If in the future we want to count their
   * usage, we need to enforce evacuator to skip freelist to avoid they eat up
   * application's cache portion. */
  return region_map_.size();
  // return region_map_.size() + freelist_.size();
}

inline uint64_t ResourceManager::NumRegionLimit() const noexcept {
  return region_limit_;
}

inline int64_t ResourceManager::NumRegionAvail() const noexcept {
  return static_cast<int64_t>(NumRegionLimit()) - NumRegionInUse();
}

inline bool ResourceManager::reclaim_trigger() const noexcept {
  float avail_ratio =
      static_cast<float>(NumRegionAvail() + 1) / (NumRegionLimit() + 1);
  if (avail_ratio < 0.1) {
    return true;
  }
  return false;
}

inline ResourceManager *ResourceManager::global_manager() noexcept {
  return global_manager_shared_ptr().get();
}

} // namespace midas
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

inline bool ResourceManager::reclaim_trigger() noexcept {
  return reclaim_target() > 0;
}

inline int64_t ResourceManager::reclaim_target() noexcept {
  int64_t nr_avail = NumRegionAvail();
  int64_t nr_limit = NumRegionLimit();
  if (nr_limit <= 1)
    return 0;
  float avail_ratio = nr_limit ? (static_cast<float>(nr_avail) / nr_limit) : 0.;
  int64_t nr_to_reclaim = nr_pending_;
  // if (avail_ratio < 0.1 || nr_avail <= 512)
  // if ((nr_limit < 5120 && nr_avail <= 512) || (nr_avail <= 768)) // wiredtiger config
  // if (avail_ratio < 0.01 || nr_avail <= 1)
  auto headroom = std::min<int64_t>(
      768ll, std::max<int64_t>(1ll, alloc_tput_stats_.alloc_tput * 0.2));
  alloc_tput_stats_.headroom = headroom;
  if (avail_ratio < 0.05 || nr_avail <= headroom)
    nr_to_reclaim += std::max(nr_limit / 50, 2l);
  nr_to_reclaim = std::min(nr_to_reclaim, nr_limit);
  return nr_to_reclaim;
}

inline ResourceManager *ResourceManager::global_manager() noexcept {
  return global_manager_shared_ptr().get();
}

} // namespace midas
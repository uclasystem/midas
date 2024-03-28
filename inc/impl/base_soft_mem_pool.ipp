#pragma once

namespace midas {

inline void BaseSoftMemPool::CacheStats::reset() noexcept {
  hits = 0;
  misses = 0;
  miss_cycles = 0;
  miss_bytes = 0;
  victim_hits = 0;
}

inline void BaseSoftMemPool::update_limit(size_t limit_in_bytes) {
  rmanager_->UpdateLimit(limit_in_bytes);
}

inline void BaseSoftMemPool::set_weight(float weight) {
  rmanager_->SetWeight(weight);
}

inline void BaseSoftMemPool::set_lat_critical(bool value) {
  rmanager_->SetLatCritical(value);
}

inline void BaseSoftMemPool::inc_cache_hit() noexcept { stats.hits++; }

inline void BaseSoftMemPool::inc_cache_miss() noexcept { stats.misses++; }

inline void
BaseSoftMemPool::inc_cache_victim_hit(ObjectPtr *optr_addr) noexcept {
  stats.victim_hits++;
  if (optr_addr)
    vcache_->get(optr_addr);
}

inline void BaseSoftMemPool::record_miss_penalty(uint64_t cycles,
                                                 uint64_t bytes) noexcept {
  stats.miss_cycles += cycles;
  stats.miss_bytes += bytes;
}

inline VictimCache *BaseSoftMemPool::get_vcache() const noexcept {
  return vcache_.get();
}

inline LogAllocator *BaseSoftMemPool::get_allocator() const noexcept {
  return allocator_.get();
}

inline Evacuator *BaseSoftMemPool::get_evacuator() const noexcept {
  return evacuator_.get();
}

inline ResourceManager *BaseSoftMemPool::get_rmanager() const noexcept {
  return rmanager_.get();
}

inline void BaseSoftMemPool::profile_stats(StatsMsg *msg) noexcept {
  auto curr_ts = Time::get_us_stt();
  auto hit_ratio = static_cast<float>(stats.hits) / (stats.hits + stats.misses);
  auto miss_penalty =
      stats.miss_bytes
          ? (static_cast<float>(stats.miss_cycles) / stats.miss_bytes)
          : 0.0;
  auto recon_time =
      static_cast<float>(stats.miss_cycles) / stats.misses / kCPUFreq;
  auto victim_hit_ratio = static_cast<float>(stats.hits + stats.victim_hits) /
                          (stats.hits + stats.victim_hits + stats.misses);
  auto victim_hits = stats.victim_hits.load();
  auto perf_gain = victim_hits * miss_penalty;

  if (msg) {
    msg->hits = stats.hits;
    msg->misses = stats.misses;
    msg->miss_penalty = miss_penalty;
    msg->vhits = victim_hits;
  }

  if (stats.hits > 0 || stats.misses > 0 || stats.victim_hits > 0)
    MIDAS_LOG_PRINTF(kInfo,
                     "CachePool %s:\n"
                     "\t     Region used: %ld/%ld\n"
                     "\tCache hit ratio:  %.4f\n"
                     "\t   miss penalty:  %.2f\n"
                     "\t construct time:  %.2f\n"
                     "\t     hit counts:  %lu\n"
                     "\t    miss counts:  %lu\n"
                     "\tVictim hit ratio: %.4f\n"
                     "\t       hit count: %lu\n"
                     "\t       perf gain: %.4f\n"
                     "\t           count: %lu\n"
                     "\t            size: %lu\n",
                     name_.c_str(), get_rmanager()->NumRegionInUse(),
                     get_rmanager()->NumRegionLimit(), hit_ratio, miss_penalty,
                     recon_time, stats.hits.load(), stats.misses.load(),
                     victim_hit_ratio, victim_hits, perf_gain, vcache_->count(),
                     vcache_->size());

  stats.timestamp = curr_ts;
  stats.reset();
}
} // namespace midas
#include <chrono>

#include "cache_manager.hpp"
#include "shm_types.hpp"
#include "time.hpp"

namespace midas {
inline void CachePool::log_stats() const noexcept {
  auto hit_ratio = static_cast<float>(stats.hits) / (stats.hits + stats.misses);
  auto miss_penalty = static_cast<float>(stats.miss_cycles) / stats.miss_bytes;
  auto victim_hit_ratio = static_cast<float>(stats.hits + stats.victim_hits) /
                          (stats.hits + stats.victim_hits + stats.misses);
  auto victim_hits = stats.victim_hits.load();
  auto perf_gain = victim_hits * miss_penalty;
  MIDAS_LOG_PRINTF(kError,
                   "CachePool %s:\n"
                   "\tCache hit ratio:  %.4f\n"
                   "\t   miss penalty:  %.2f\n"
                   "\t     hit counts:  %lu\n"
                   "\t    miss counts:  %lu\n"
                   "\tVictim hit ratio: %.4f\n"
                   "\t       hit count: %lu\n"
                   "\t       perf gain: %.4f\n"
                   "\t           count: %lu\n"
                   "\t            size: %lu\n",
                   name_.c_str(), hit_ratio, miss_penalty, stats.hits.load(),
                   stats.misses.load(), victim_hit_ratio, victim_hits,
                   perf_gain, vcache_->count(), vcache_->size());
}

inline void CachePool::CacheStats::reset() noexcept {
  hits = 0;
  misses = 0;
  miss_cycles = 0;
  miss_bytes = 0;
  victim_hits = 0;
}

StatsMsg CacheManager::profile_pools() {
  StatsMsg stats{0};
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, pool] : pools_) {
    uint64_t curr_ts = Time::get_us_stt();
    uint64_t prev_ts = pool->stats.timestamp;
    if (pool->stats.hits) {
      pool->log_stats();
      stats.hits = pool->stats.hits;
      stats.misses = pool->stats.misses;
      stats.miss_penalty =
          static_cast<double>(pool->stats.miss_cycles) / pool->stats.miss_bytes;
      stats.vhits = pool->stats.victim_hits;
      // stats.vmisses = 0;
    }
    pool->stats.reset();
    pool->stats.timestamp = curr_ts;
  }
  ul.unlock();
  return stats;
}

bool CacheManager::create_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (pools_.find(name) != pools_.cend()) {
    MIDAS_LOG(kError) << "CachePool " << name << " has already been created!";
    return false;
  }
  auto pool = std::make_unique<CachePool>(name);
  MIDAS_LOG(kInfo) << "Create cache pool " << name;
  pools_[name] = std::move(pool);
  return true;
}

bool CacheManager::delete_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (pools_.find(name) == pools_.cend()) {
    MIDAS_LOG(kError) << "CachePool " << name << " has already been deleted!";
    return false;
  }
  pools_.erase(name);
  return true;
}
} // namespace midas
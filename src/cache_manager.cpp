#include <chrono>

#include "cache_manager.hpp"
#include "shm_types.hpp"
#include "sig_handler.hpp"
#include "time.hpp"

namespace midas {
inline void CachePool::profile_stats(StatsMsg *msg) noexcept {
  if (stats.hits == 0)
    return;
  auto curr_ts = Time::get_us_stt();
  auto hit_ratio = static_cast<float>(stats.hits) / (stats.hits + stats.misses);
  auto miss_penalty =
      stats.miss_bytes
          ? (static_cast<float>(stats.miss_cycles) / stats.miss_bytes)
          : 0.0;
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

  MIDAS_LOG_PRINTF(kInfo,
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

  stats.timestamp = curr_ts;
  stats.reset();
}

inline void CachePool::CacheStats::reset() noexcept {
  hits = 0;
  misses = 0;
  miss_cycles = 0;
  miss_bytes = 0;
  victim_hits = 0;
}

CacheManager::CacheManager() : terminated_(false), profiler_(nullptr) {
  assert(create_pool(default_pool_name));
  auto sig_handler = SigHandler::global_sighandler();
  sig_handler->init();
  // profiler_ = std::make_unique<std::thread>([&] {
  //   constexpr static uint64_t PROF_INTERVAL = 2; // about 2s
  //   while (!terminated_) {
  //     std::this_thread::sleep_for(std::chrono::seconds(PROF_INTERVAL));
  //     profile_pools();
  //   }
  // });
}

StatsMsg CacheManager::profile_pools() {
  StatsMsg stats{0};
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, pool] : pools_) {
    uint64_t curr_ts = Time::get_us_stt();
    uint64_t prev_ts = pool->stats.timestamp;
    if (pool->stats.hits) {
      pool->profile_stats(&stats);
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
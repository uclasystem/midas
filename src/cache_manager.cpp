#include <chrono>

#include "cache_manager.hpp"
#include "sig_handler.hpp"
#include "utils.hpp"

namespace midas {

CacheManager::CacheManager() : terminated_(false), profiler_(nullptr) {
  // assert(create_pool(default_pool_name));
  auto sig_handler = SigHandler::global_sighandler();
  sig_handler->init();
}

StatsMsg CacheManager::profile_pools() {
  StatsMsg stats{0};
  std::unique_lock<std::mutex> ul(mtx_);
  for (auto &[_, pool] : pools_) {
    uint64_t curr_ts = Time::get_us_stt();
    uint64_t prev_ts = pool->stats.timestamp;
    if (pool->stats.hits || pool->stats.misses) {
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
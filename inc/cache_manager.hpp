#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "evacuator.hpp"
#include "log.hpp"
#include "object.hpp"

namespace cachebank {

class CachePool {
public:
  CachePool(std::string name);
  ~CachePool();

  using ConstructFunc = std::function<int(void *)>;
  void set_construct_func(ConstructFunc callback);
  ConstructFunc get_construct_func() const noexcept;
  int construct(void *arg);

  // Profiling
  void inc_cache_hit();
  void inc_cache_miss();
  void record_miss_penalty(uint64_t cycles, uint64_t bytes);

  inline LogAllocator *get_allocator() const noexcept;
  inline Evacuator *get_evacuator() const noexcept;

  static CachePool *global_cache_pool();

private:
  void log_stats() const noexcept;

  std::string name_;
  ConstructFunc construct_;

  // Stats & Counters
  struct CacheStats {
    std::atomic_uint_fast64_t hits{0};
    std::atomic_uint_fast64_t misses{0};
    std::atomic_uint_fast64_t miss_cycles{0};
    std::atomic_uint_fast64_t miss_bytes{0};

    void reset() noexcept;
  } stats;

  std::shared_ptr<LogAllocator> allocator_;
  std::unique_ptr<Evacuator> evacuator_;
};

class CacheManager {
public:
  CacheManager();
  ~CacheManager();

  bool create_pool(std::string name);
  bool delete_pool(std::string name);
  CachePool *get_pool(std::string name);

  size_t num_pools() const noexcept;

  static CacheManager *global_cache_manager();

  constexpr static char default_pool_name[] = "default";

private:
  std::mutex mtx_;
  std::unordered_map<std::string, std::unique_ptr<CachePool>> pools_;
};

} // namespace cachebank

#include "impl/cache_manager.ipp"
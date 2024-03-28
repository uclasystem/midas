#pragma once

#include "evacuator.hpp"
#include "log.hpp"
#include "resource_manager.hpp"
#include "shm_types.hpp"
#include "time.hpp"
#include "victim_cache.hpp"

namespace midas {

class BaseSoftMemPool {
public:
  virtual ~BaseSoftMemPool() = default;

  // Config
  inline void update_limit(size_t limit_in_bytes);
  inline void set_weight(float weight);
  inline void set_lat_critical(bool value);

  // Profiling
  inline void inc_cache_hit() noexcept;
  inline void inc_cache_miss() noexcept;
  inline void inc_cache_victim_hit(ObjectPtr *optr_addr = nullptr) noexcept;
  inline void record_miss_penalty(uint64_t cycles, uint64_t bytes) noexcept;
  inline void profile_stats(StatsMsg *msg = nullptr) noexcept;

  inline VictimCache *get_vcache() const noexcept;
  inline ResourceManager *get_rmanager() const noexcept;
  inline LogAllocator *get_allocator() const noexcept;
  inline Evacuator *get_evacuator() const noexcept;

protected:
  std::string name_;
  // Stats & Counters
  struct CacheStats {
    std::atomic_uint_fast64_t hits{0};
    std::atomic_uint_fast64_t misses{0};
    std::atomic_uint_fast64_t miss_cycles{0};
    std::atomic_uint_fast64_t miss_bytes{0};
    std::atomic_uint_fast64_t victim_hits{0};
    uint64_t timestamp{0};

    inline void reset() noexcept;
  } stats;

  std::unique_ptr<VictimCache> vcache_;
  std::shared_ptr<ResourceManager> rmanager_;
  std::shared_ptr<LogAllocator> allocator_;
  std::unique_ptr<Evacuator> evacuator_;

  friend class CacheManager;
  friend class ResourceManager;

  static constexpr uint64_t kVCacheSizeLimit = 64 * 1024 * 1024; // 64 MB
  static constexpr uint64_t kVCacheCountLimit = 500000;
};

} // namespace midas

#include "impl/base_soft_mem_pool.ipp"
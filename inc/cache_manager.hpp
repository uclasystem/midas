#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "evacuator.hpp"
#include "log.hpp"
#include "object.hpp"
#include "resource_manager.hpp"
#include "shm_types.hpp"
#include "time.hpp"
#include "victim_cache.hpp"

namespace midas {

struct ConstructPlug {
  uint64_t stt_cycles{0};
  uint64_t end_cycles{0};
  int64_t bytes{0};

  void reset() {
    stt_cycles = 0;
    end_cycles = 0;
    bytes = 0;
  }
};

class CachePool {
public:
  CachePool(std::string name);
  ~CachePool();

  // Config
  void update_limit(size_t limit_in_bytes);
  void set_weight(float weight);
  void set_lat_critical(bool value);

  // Callback Functions
  using ConstructFunc = std::function<int(void *)>;
  void set_construct_func(ConstructFunc callback);
  ConstructFunc get_construct_func() const noexcept;
  int construct(void *arg);
  using PreevictFunc = std::function<int(ObjectPtr *)>;
  int preevict(ObjectPtr *optr);
  PreevictFunc get_preevict_func() const noexcept;
  using DestructFunc = std::function<int(ObjectPtr *)>;
  int destruct(ObjectPtr *optr);
  DestructFunc get_destruct_func() const noexcept;

  // Construct helper functions for recording penalty
  void construct_stt(ConstructPlug &plug) noexcept; // mark stt/end of
  void construct_end(ConstructPlug &plug) noexcept; // the construct path
  void construct_add(uint64_t bytes,
                     ConstructPlug &plug) noexcept; // add up missed bytes

  // Allocator shortcuts
  inline std::optional<ObjectPtr> alloc(size_t size);
  inline bool alloc_to(size_t size, ObjectPtr *dst);
  inline bool free(ObjectPtr &ptr);

  // Profiling
  void inc_cache_hit() noexcept;
  void inc_cache_miss() noexcept;
  void inc_cache_victim_hit(ObjectPtr *optr_addr = nullptr) noexcept; // TODO: check all callsites
  void record_miss_penalty(uint64_t cycles, uint64_t bytes) noexcept;
  void profile_stats(StatsMsg *msg = nullptr) noexcept;

  inline VictimCache *get_vcache() const noexcept;
  inline ResourceManager *get_rmanager() const noexcept;
  inline LogAllocator *get_allocator() const noexcept;
  inline Evacuator *get_evacuator() const noexcept;

  static inline CachePool *global_cache_pool();

private:
  std::string name_;
  ConstructFunc construct_;
  PreevictFunc preevict_;
  DestructFunc destruct_;

  // Stats & Counters
  struct CacheStats {
    std::atomic_uint_fast64_t hits{0};
    std::atomic_uint_fast64_t misses{0};
    std::atomic_uint_fast64_t miss_cycles{0};
    std::atomic_uint_fast64_t miss_bytes{0};
    std::atomic_uint_fast64_t victim_hits{0};
    uint64_t timestamp{0};

    void reset() noexcept;
  } stats;

  std::unique_ptr<VictimCache> vcache_;
  std::shared_ptr<ResourceManager> rmanager_;
  std::shared_ptr<LogAllocator> allocator_;
  std::unique_ptr<Evacuator> evacuator_;

  friend class CacheManager;
  friend class ResourceManager;

  static constexpr uint64_t kVCacheSizeLimit = 32 * 1024 * 1024; // 32 MB
  static constexpr uint64_t kVCacheCountLimit = 500000;
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
  bool terminated_;
  StatsMsg profile_pools();
  std::unique_ptr<std::thread> profiler_;

  std::mutex mtx_;
  std::unordered_map<std::string, std::unique_ptr<CachePool>> pools_;

  friend class ResourceManager;
};

} // namespace midas

#include "impl/cache_manager.ipp"
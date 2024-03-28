#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>

#include "base_soft_mem_pool.hpp"
#include "object.hpp"
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

class CachePool : public BaseSoftMemPool {
public:
  CachePool(std::string name);
  ~CachePool();

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

  static inline CachePool *global_cache_pool();

private:
  ConstructFunc construct_;
  PreevictFunc preevict_;
  DestructFunc destruct_;
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
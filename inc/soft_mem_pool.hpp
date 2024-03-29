#pragma once

#include <memory>
#include <mutex>

#include "base_soft_mem_pool.hpp"
#include "soft_unique_ptr.hpp"

namespace midas {

template <typename T, typename... ReconArgs>
class SoftMemPool : public BaseSoftMemPool {
public:
  using ReconFunc = std::function<T(ReconArgs...)>;

  SoftUniquePtr<T, ReconArgs...> new_unique();
  T reconstruct(ReconArgs... args);
  bool alloc_to(size_t size, ObjectPtr *ptr);

  static inline SoftMemPool<T, ReconArgs...> *create_pool(std::string name,
                                                          ReconFunc recon_fun);

private:
  SoftMemPool(std::string name, ReconFunc recon_fun);
  friend class SoftMemPoolManager;

  ReconFunc recon_fun_;
};

class SoftMemPoolManager {
public:
  SoftMemPoolManager() = default;
  ~SoftMemPoolManager();

  size_t num_pools() noexcept;
  template <typename T, typename... ReconArgs>
  bool create_pool(std::string name,
                   typename SoftMemPool<T, ReconArgs...>::ReconFunc recon_fun);
  template <typename T, typename... ReconArgs>
  SoftMemPool<T, ReconArgs...> *get_pool(std::string name);
  bool delete_pool(std::string name);

  static SoftMemPoolManager *global_soft_mem_pool_manager();

private:
  std::mutex mtx_;
  std::unordered_map<std::string, std::unique_ptr<BaseSoftMemPool>> pools_;
};

} // namespace midas

#include "impl/soft_mem_pool.ipp"
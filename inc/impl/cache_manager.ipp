#pragma once

namespace cachebank {
inline CachePool::CachePool(std::string name)
    : name_(name), construct_(nullptr) {
  allocator_ = std::make_shared<LogAllocator>();
  evacuator_ = std::make_unique<Evacuator>(allocator_);
}

inline CachePool::~CachePool() {}

inline CachePool *CachePool::global_cache_pool() {
  auto cache_mgr = CacheManager::global_cache_manager();
  if (!cache_mgr)
    return nullptr;
  auto pool = cache_mgr->get_pool(CacheManager::default_pool_name);
  if (pool)
    return pool;
  if (!cache_mgr->create_pool())
    return nullptr;
  return cache_mgr->get_pool(CacheManager::default_pool_name);
}

inline void CachePool::set_construct_func(ConstructFunc callback) {
  if (construct_)
    LOG(kWarning) << "Cache pool " << name_
                  << " has already set its construct callback";
  else
    construct_ = callback;
}

inline CachePool::ConstructFunc CachePool::get_construct_func() const noexcept {
  return construct_;
}

inline int CachePool::construct(void *arg) { return construct_(arg); };

inline void CachePool::inc_cache_hit() { stats.hits++; }

inline void CachePool::inc_cache_miss() { stats.misses++; }

inline void CachePool::record_miss_penalty(uint64_t cycles, uint64_t bytes) {
  stats.miss_cycles += cycles;
  stats.miss_bytes += bytes;
}

inline LogAllocator *CachePool::get_allocator() const noexcept {
  return allocator_.get();
}

inline Evacuator *CachePool::get_evacuator() const noexcept {
  return evacuator_.get();
}

inline CacheManager::CacheManager() { assert(create_pool()); }

inline CacheManager::~CacheManager() { pools_.clear(); }

inline bool CacheManager::create_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (pools_.find(name) != pools_.cend()) {
    LOG(kError) << "CachePool " << name << " has already been created!";
    return false;
  }
  auto pool = std::make_unique<CachePool>(name);
  LOG(kInfo) << "Create cache pool " << name;
  pools_[name] = std::move(pool);
  return true;
}

inline bool CacheManager::delete_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (pools_.find(name) == pools_.cend()) {
    LOG(kError) << "CachePool " << name << " has already been deleted!";
    return false;
  }
  pools_.erase(name);
  return true;
}

inline CachePool *CacheManager::get_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto found = pools_.find(name);
  if (found == pools_.cend())
    return nullptr;
  return found->second.get();
}

inline size_t CacheManager::num_pools() const noexcept { return pools_.size(); }

inline CacheManager *CacheManager::global_cache_manager() {
  static std::mutex mtx_;
  static std::unique_ptr<CacheManager> manager_;

  if (manager_.get())
    return manager_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (manager_.get())
    return manager_.get();
  manager_ = std::make_unique<CacheManager>();
  return manager_.get();
}

} // namespace cachebank
#pragma once

namespace cachebank {
inline CachePool::CachePool(std::string name)
    : name_(name), hits_(0), misses_(0), miss_cycles_(0), miss_bytes_(0) {
  allocator_ = std::make_shared<LogAllocator>();
  evacuator_ = std::make_unique<Evacuator>(allocator_);
}

inline CachePool::~CachePool() {}

inline CachePool *CachePool::global_cache_pool() {
  static std::mutex mtx_;
  static std::unique_ptr<CachePool> pool_;

  if (pool_.get())
    return pool_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (pool_.get())
    return pool_.get();
  pool_ = std::make_unique<CachePool>("global");
  return pool_.get();
}

inline void CachePool::record_miss(uint64_t cycles, uint64_t bytes) {
  miss_cycles_ += cycles;
  miss_bytes_ += bytes;
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
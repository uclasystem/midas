#pragma once

namespace midas {
inline CachePool::CachePool(std::string name)
    : name_(name), construct_(nullptr) {
  vcache_ = std::make_unique<VictimCache>(kVCacheSizeLimit, kVCacheCountLimit);
  allocator_ = std::make_shared<LogAllocator>(this);
  evacuator_ = std::make_unique<Evacuator>(this, allocator_);
}

inline CachePool::~CachePool() {}

inline CachePool *CachePool::global_cache_pool() {
  static std::mutex mtx_;
  static CachePool *pool_ = nullptr;
  if (pool_)
    return pool_;
  std::unique_lock<std::mutex> ul(mtx_);
  if (pool_)
    return pool_;
  ul.unlock();
  auto cache_mgr = CacheManager::global_cache_manager();
  if (!cache_mgr)
    return nullptr;
  auto pool = cache_mgr->get_pool(CacheManager::default_pool_name);
  if (pool)
    return pool;
  else if (!cache_mgr->create_pool(CacheManager::default_pool_name))
    return nullptr;
  ul.lock();
  pool_ = cache_mgr->get_pool(CacheManager::default_pool_name);
  return pool_;
}

inline void CachePool::set_construct_func(ConstructFunc callback) {
  if (construct_)
    MIDAS_LOG(kWarning) << "Cache pool " << name_
                        << " has already set its construct callback";
  else
    construct_ = callback;
}

inline CachePool::ConstructFunc CachePool::get_construct_func() const noexcept {
  return construct_;
}

inline int CachePool::construct(void *arg) { return construct_(arg); };

inline std::optional<ObjectPtr> CachePool::alloc(size_t size) {
  return allocator_->alloc(size);
}

inline bool CachePool::alloc_to(size_t size, ObjectPtr *dst) {
  return allocator_->alloc_to(size, dst);
}

inline bool CachePool::free(ObjectPtr &ptr) {
  if (ptr.is_victim())
    vcache_->remove(&ptr);
  return allocator_->free(ptr);
}

inline void CachePool::inc_cache_hit() { stats.hits++; }

inline void CachePool::inc_cache_miss() { stats.misses++; }

inline void CachePool::inc_cache_victim_hit() { stats.victim_hits++; }

inline void CachePool::record_miss_penalty(uint64_t cycles, uint64_t bytes) {
  stats.miss_cycles += cycles;
  stats.miss_bytes += bytes;
}

inline VictimCache *CachePool::get_vcache() const noexcept {
  return vcache_.get();
}

inline LogAllocator *CachePool::get_allocator() const noexcept {
  return allocator_.get();
}

inline Evacuator *CachePool::get_evacuator() const noexcept {
  return evacuator_.get();
}

inline CacheManager::CacheManager() : terminated_(false), profiler_(nullptr) {
  assert(create_pool(default_pool_name));
  // profiler_ = std::make_unique<std::thread>([&] {
  //   constexpr static uint64_t PROF_INTERVAL = 2; // about 2s
  //   while (!terminated_) {
  //     std::this_thread::sleep_for(std::chrono::seconds(PROF_INTERVAL));
  //     profile_pools();
  //   }
  // });
}

inline CacheManager::~CacheManager() {
  terminated_ = true;
  if (profiler_)
    profiler_->join();
  pools_.clear();
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

} // namespace midas
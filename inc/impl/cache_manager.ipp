#pragma once

namespace midas {
inline CachePool::CachePool(std::string name)
    : name_(name), construct_(nullptr) {
  vcache_ = std::make_unique<VictimCache>(kVCacheSizeLimit, kVCacheCountLimit);
  allocator_ = std::make_shared<LogAllocator>(this);
  rmanager_ = std::make_shared<ResourceManager>(this);
  evacuator_ = std::make_unique<Evacuator>(this, rmanager_, allocator_);
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

inline void CachePool::update_limit(size_t limit_in_bytes) {
  rmanager_->UpdateLimit(limit_in_bytes);
}

inline void CachePool::set_weight(float weight) {
  rmanager_->SetWeight(weight);
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

inline void CachePool::construct_stt(ConstructPlug &plug) noexcept {
  plug.reset();
  plug.stt_cycles = Time::get_cycles_stt();
}

inline void CachePool::construct_end(ConstructPlug &plug) noexcept {
  plug.end_cycles = Time::get_cycles_end();
  auto cycles = plug.end_cycles - plug.stt_cycles;
  if (plug.bytes && cycles)
    record_miss_penalty(cycles, plug.bytes);
}

inline void CachePool::construct_add(uint64_t bytes,
                                     ConstructPlug &plug) noexcept {
  plug.bytes += bytes;
}

inline bool CachePool::alloc_to(size_t size, ObjectPtr *dst) {
  return allocator_->alloc_to(size, dst);
}

inline bool CachePool::free(ObjectPtr &ptr) {
  if (ptr.is_victim())
    vcache_->remove(&ptr);
  return allocator_->free(ptr);
}

inline void CachePool::inc_cache_hit() noexcept { stats.hits++; }

inline void CachePool::inc_cache_miss() noexcept { stats.misses++; }

inline void CachePool::inc_cache_victim_hit(ObjectPtr *optr_addr) noexcept {
  stats.victim_hits++;
  if (optr_addr)
    vcache_->get(optr_addr);
}

inline void CachePool::record_miss_penalty(uint64_t cycles,
                                           uint64_t bytes) noexcept {
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

inline ResourceManager *CachePool::get_rmanager() const noexcept {
  return rmanager_.get();
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
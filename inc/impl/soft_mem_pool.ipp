#pragma once

namespace midas {

template <typename T, typename... ReconArgs>
SoftMemPool<T, ReconArgs...>::SoftMemPool(std::string name, ReconFunc recon_fun)
    : recon_fun_(recon_fun) {
  name_ = name;
  vcache_ = std::make_unique<VictimCache>(kVCacheSizeLimit, kVCacheCountLimit);
  allocator_ = std::make_shared<LogAllocator>(this);
  rmanager_ = std::make_shared<ResourceManager>(this);
  evacuator_ = std::make_unique<Evacuator>(this, rmanager_, allocator_);
}

template <typename T, typename... ReconArgs>
SoftUniquePtr<T, ReconArgs...> SoftMemPool<T, ReconArgs...>::new_unique() {
  return SoftUniquePtr<T, ReconArgs...>(this);
}

template <typename T, typename... ReconArgs>
T SoftMemPool<T, ReconArgs...>::reconstruct(ReconArgs... args) {
  auto stt = Time::get_cycles_stt();
  T value = recon_fun_(args...);
  auto end = Time::get_cycles_end();
  record_miss_penalty(end - stt, sizeof(T));
  return value;
}

template <typename T, typename... ReconArgs>
bool SoftMemPool<T, ReconArgs...>::alloc_to(size_t size, ObjectPtr *ptr) {
  return allocator_->alloc_to(size, ptr);
}

SoftMemPoolManager::~SoftMemPoolManager() { pools_.clear(); };

inline size_t SoftMemPoolManager::num_pools() noexcept {
  std::unique_lock<std::mutex> ul(mtx_);
  return pools_.size();
}

template <typename T, typename... ReconArgs>
bool SoftMemPoolManager::create_pool(
    std::string name,
    typename SoftMemPool<T, ReconArgs...>::ReconFunc recon_fun) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (pools_.find(name) != pools_.cend()) {
    MIDAS_LOG(kError) << "SoftMemPool " << name << " has already been created!";
    return false;
  }
  auto pool = new SoftMemPool<T, ReconArgs...>(name, recon_fun);
  pools_[name] = std::unique_ptr<SoftMemPool<T, ReconArgs...>>(pool);
  return true;
}

template <typename T, typename... ReconArgs>
SoftMemPool<T, ReconArgs...> *SoftMemPoolManager::get_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto it = pools_.find(name);
  if (it == pools_.cend()) {
    MIDAS_LOG(kError) << "SoftMemPool " << name << " does not exist!";
    return nullptr;
  }
  return dynamic_cast<SoftMemPool<T, ReconArgs...> *>(it->second.get());
}

bool SoftMemPoolManager::delete_pool(std::string name) {
  std::unique_lock<std::mutex> ul(mtx_);
  auto it = pools_.find(name);
  if (it == pools_.cend()) {
    MIDAS_LOG(kError) << "SoftMemPool " << name << " does not exist!";
    return false;
  }
  pools_.erase(it);
  MIDAS_LOG(kInfo) << "SoftMemPool " << name << " has been deleted!";
  return true;
}

inline SoftMemPoolManager *SoftMemPoolManager::global_soft_mem_pool_manager() {
  static std::mutex mtx_;
  static std::unique_ptr<SoftMemPoolManager> manager_;

  if (manager_.get())
    return manager_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (manager_.get())
    return manager_.get();
  manager_ = std::make_unique<SoftMemPoolManager>();
  return manager_.get();
}

} // namespace midas
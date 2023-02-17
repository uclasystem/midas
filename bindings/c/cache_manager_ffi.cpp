#include "cache_manager.h"

#include "../../inc/cache_manager.hpp"

bool midas_create_cache_pool(const char name[], CachePool *pool) {
  auto c_mgr = cachebank::CacheManager::global_cache_manager();
  return c_mgr->create_pool(name);
}

bool midas_delete_cache_pool(const char name[]) {
  auto c_mgr = cachebank::CacheManager::global_cache_manager();
  return c_mgr->delete_pool(name);
}

bool midas_get_cache_pool(const char name[], CachePool *pool) {
  auto c_mgr = cachebank::CacheManager::global_cache_manager();
  *pool = c_mgr->get_pool(name);
  return *pool != nullptr;
}

bool midas_get_global_cache_pool(CachePool *pool) {
  *pool = cachebank::CachePool::global_cache_pool();
  return *pool != nullptr;
}

void midas_record_miss(CachePool pool, uint64_t cycles, uint64_t bytes) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (pool_)
    pool_->record_miss(cycles, bytes);
}

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

CachePool midas_get_cache_pool(const char name[]) {
  auto c_mgr = cachebank::CacheManager::global_cache_manager();
  return c_mgr->get_pool(name);
}

CachePool midas_get_global_cache_pool(void) {
  return cachebank::CachePool::global_cache_pool();
}

void midas_pool_set_construct_func(CachePool pool,
                                   midas_construct_func_t callback) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (pool_)
    pool_->set_construct_func(callback);
}

bool midas_pool_has_construct_func(CachePool pool) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (!pool_)
    return false;
  return (bool)pool_->get_construct_func();
}

int midas_pool_construct(CachePool pool, void *arg) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (!pool_)
    return -1;
  return pool_->construct(arg);
}

void midas_inc_cache_hit(CachePool *pool) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (pool_)
    pool_->inc_cache_hit();
}

void midas_inc_cache_miss(CachePool *pool) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  if (pool_)
    pool_->inc_cache_hit();
}

void midas_record_miss_penalty(CachePool pool, uint64_t cycles,
                               uint64_t bytes) {
  auto pool_ = reinterpret_cast<cachebank::CachePool *>(pool);
  assert(pool_);
  if (pool_)
    pool_->record_miss_penalty(cycles, bytes);
}

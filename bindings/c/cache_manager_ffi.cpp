#include "cache_manager.h"

#include "../../inc/cache_manager.hpp"

bool midas_create_cache_pool(const char name[]) {
  auto c_mgr = midas::CacheManager::global_cache_manager();
  return c_mgr->create_pool(name);
}

bool midas_delete_cache_pool(const char name[]) {
  auto c_mgr = midas::CacheManager::global_cache_manager();
  return c_mgr->delete_pool(name);
}

cache_pool_t midas_get_cache_pool(const char name[]) {
  auto c_mgr = midas::CacheManager::global_cache_manager();
  return c_mgr->get_pool(name);
}

cache_pool_t midas_get_global_cache_pool(void) {
  return midas::CachePool::global_cache_pool();
}

void midas_pool_set_construct_func(cache_pool_t pool,
                                   midas_construct_func_t callback) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (pool_)
    pool_->set_construct_func(callback);
}

bool midas_pool_has_construct_func(cache_pool_t pool) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return false;
  return (bool)pool_->get_construct_func();
}

int midas_pool_construct(cache_pool_t pool, void *arg) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return -1;
  return pool_->construct(arg);
}

int midas_pool_update_limit(cache_pool_t pool, uint64_t limit_in_bytes) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return -1;
  pool_->update_limit(limit_in_bytes);
  return 0;
}

int midas_pool_set_weight(cache_pool_t pool, int32_t weight) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return -1;
  pool_->set_weight(weight);
  return 0;
}

void midas_inc_cache_hit(cache_pool_t pool) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (pool_)
    pool_->inc_cache_hit();
}

void midas_inc_cache_miss(cache_pool_t pool) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (pool_)
    pool_->inc_cache_miss();
}

void midas_inc_cache_victim_hit(cache_pool_t pool) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (pool_)
    pool_->inc_cache_victim_hit();
}

void midas_record_miss_penalty(cache_pool_t pool, uint64_t cycles,
                               uint64_t bytes) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  assert(pool_);
  if (pool_)
    pool_->record_miss_penalty(cycles, bytes);
}

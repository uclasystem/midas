#ifndef __MIDAS_CACHE_MANAGER_H__
#define __MIDAS_CACHE_MANAGER_H__

#include <stdbool.h>

#include "midas_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *cache_pool_t;

bool midas_create_cache_pool(const char name[]);
bool midas_delete_cache_pool(const char name[]);
cache_pool_t midas_get_cache_pool(const char name[]);
cache_pool_t midas_get_global_cache_pool(void);
int midas_pool_update_limit(cache_pool_t pool, uint64_t limit_in_bytes);
int midas_pool_set_weight(cache_pool_t pool, int32_t weight);

typedef int (*midas_construct_func_t)(void *arg);

void midas_pool_set_construct_func(cache_pool_t pool,
                                   midas_construct_func_t callback);
bool midas_pool_get_construct_func(cache_pool_t pool);
int midas_pool_construct(cache_pool_t pool, void *arg);

void midas_inc_cache_hit(cache_pool_t pool);
void midas_inc_cache_miss(cache_pool_t pool);
void midas_inc_cache_victim_hit(cache_pool_t pool);
void midas_record_miss_penalty(cache_pool_t pool, uint64_t cycles,
                               uint64_t bytes);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_CACHE_MANAGER_H__
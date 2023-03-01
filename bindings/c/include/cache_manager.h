#ifndef __MIDAS_CACHE_MANAGER_H__
#define __MIDAS_CACHE_MANAGER_H__

#include <stdbool.h>

#include "midas_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *CachePool;

bool midas_create_cache_pool(const char name[]);
bool midas_delete_cache_pool(const char name[]);
CachePool midas_get_cache_pool(const char name[]);
CachePool midas_get_global_cache_pool(void);

typedef int (*midas_construct_func_t)(void *arg);

void midas_pool_set_construct_func(CachePool pool, midas_construct_func_t callback);
bool midas_pool_get_construct_func(CachePool pool);
int midas_pool_construct(CachePool pool, void *arg);

void midas_inc_cache_hit(CachePool pool);
void midas_inc_cache_miss(CachePool pool);
void midas_inc_cache_victim_hit(CachePool pool);
void midas_record_miss_penalty(CachePool pool, uint64_t cycles, uint64_t bytes);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_CACHE_MANAGER_H__
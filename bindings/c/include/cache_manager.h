#ifndef __MIDAS_CACHE_MANAGER_H__
#define __MIDAS_CACHE_MANAGER_H__

#include "midas_utils.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void *CachePool;

bool midas_create_cache_pool(const char name[]);
bool midas_delete_cache_pool(const char name[]);
bool midas_get_cache_pool(const char name[], CachePool *pool);

bool midas_get_global_cache_pool(CachePool *pool);

void midas_inc_cache_hit(CachePool *pool);
void midas_inc_cache_miss(CachePool *pool);
void midas_record_miss_penalty(CachePool pool, uint64_t cycles, uint64_t bytes);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_CACHE_MANAGER_H__
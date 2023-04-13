#ifndef __MIDAS_SOFTMEM_H
#define __MIDAS_SOFTMEM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

#include "cache_manager.h"

typedef uint64_t object_ptr_t;

extern object_ptr_t midas_alloc_soft(const cache_pool_t pool, size_t size);
extern bool midas_free_soft(const cache_pool_t pool, object_ptr_t optr);

extern bool midas_copy_from_soft(const cache_pool_t pool, void *dest,
                                 const object_ptr_t src, size_t len,
                                 int64_t offset);
extern bool midas_copy_to_soft(const cache_pool_t pool, object_ptr_t dest,
                               const void *src, size_t len, int64_t offset);

extern bool midas_soft_ptr_null(const object_ptr_t optr);
extern bool midas_soft_ptr_is_victim(const object_ptr_t optr);
extern bool midas_soft_contains(const object_ptr_t optr, const uint64_t addr);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_SOFTMEM_H
#ifndef __MIDAS_SOFTMEM_H
#define __MIDAS_SOFTMEM_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stddef.h>
#include <stdint.h>

typedef uint64_t ObjectPtr;

extern ObjectPtr midas_alloc_soft(size_t size);
extern bool midas_free_soft(ObjectPtr optr);

extern bool midas_copy_from_soft(void *dest, const ObjectPtr src, size_t len,
                                 int64_t offset);
extern bool midas_copy_to_soft(ObjectPtr dest, const void *src, size_t len,
                               int64_t offset);

extern bool midas_soft_contains(const ObjectPtr optr, const uint64_t addr);

#ifdef __cplusplus
}
#endif

#endif // __MIDAS_SOFTMEM_H
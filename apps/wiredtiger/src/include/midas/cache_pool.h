#ifndef __MIDAS_CACHE_POOL_H
#define __MIDAS_CACHE_POOL_H

#include "wt_internal.h"

struct MidasConstructArg {
    WT_SESSION_IMPL *session;
    WT_ITEM *buf;
    const uint8_t *addr;
    bool found;
    bool blkcache_found;
    size_t addr_size;
};

int __construct_from_disk(void *arg);

#endif // __MIDAS_CACHE_POOL_H
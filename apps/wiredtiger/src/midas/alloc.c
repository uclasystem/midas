#include "wt_internal.h"

#include "softmem.h" // midas

inline int
__midas_malloc(WT_SESSION_IMPL *session, size_t bytes_to_allocate, void *retp)
{
    object_ptr_t optr = (object_ptr_t)NULL;
    cache_pool_t pool = midas_get_global_cache_pool();
    int retry = 0;
    while (true) {
        optr = midas_alloc_soft(pool, bytes_to_allocate);
        if (optr)
            break;

        usleep(1000); // sleep 1ms
        retry++;
        if (retry > 20) {
            MIDAS_PRINTF("session %p: memory allocation of %" WT_SIZET_FMT " bytes failed", session,
              bytes_to_allocate);
            retry = 0;
        }
    }

    *(object_ptr_t *)retp = optr;
    return 0;
}

inline void
__midas_free_int(WT_SESSION_IMPL *session, const void *p_arg)
  WT_GCC_FUNC_ATTRIBUTE((visibility("default")))
{
    object_ptr_t optr = *(object_ptr_t *)p_arg;
    cache_pool_t pool = midas_get_global_cache_pool();
    if (!midas_free_soft(pool, optr)) {
        if (false)
            MIDAS_PRINTF("session %p: memory free of %p failed, errno %d\n", (void *)optr, session,
              __wt_errno());
    }
}

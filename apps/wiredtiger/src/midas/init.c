#include "wt_internal.h"

#include "midas/cache_pool.h"
// [Midas]
#include "resource_manager.h"
#include "cache_manager.h"

void
midas_runtime_init(WT_SESSION_IMPL *session)
{
    resource_manager_t manager = midas_get_global_manager();
    cache_pool_t pool = midas_get_global_cache_pool();
    midas_pool_update_limit(pool, 5ull * 1024 * 1024 * 1024); // GB
    midas_pool_set_weight(pool, 10);
    // midas_pool_set_construct_func(pool, __construct_from_disk);

    fprintf(stderr, "get midas global manager @ %p, global cache pool @ %p\n", manager, pool);
    if (__wt_log_printf(
          session, "get midas global manager @ %p, global cache pool @ %p\n", manager, pool)) {
        ;
    }
}

void
midas_runtime_destroy(WT_SESSION_IMPL *session)
{
    resource_manager_t manager = midas_get_global_manager();
    cache_pool_t pool = midas_get_global_cache_pool();
    // midas_pool_set_construct_func(pool, NULL);
    fprintf(stderr, "going to destroy midas global manager @ %p, global cache pool @ %p\n", manager,
      pool);
    if (__wt_log_printf(session,
          "going to destroy midas global manager @ %p, global cache pool @ %p\n", manager, pool)) {
        ;
    }
}

#include <stdio.h>
#include <unistd.h>

#include <resource_manager.h>
#include <softmem.h>

int main(int argc, char *argv[]) {
  resource_manager_t rmanager = midas_get_global_manager();
  printf("get midas resource manager @ %p\n", rmanager);
  bool succ = midas_create_cache_pool("test");
  cache_pool_t pool;
  pool = midas_get_cache_pool("test");
  object_ptr_t optr = midas_alloc_soft(pool, 10);
  sleep(1); // leave some time for rmanager to establish connections.
  return 0;
}
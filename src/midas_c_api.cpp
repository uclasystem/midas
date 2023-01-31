#include <midas.h>
#include <resource_manager.hpp>

#ifdef __cplusplus
extern "C" {
#endif

void *midas_get_global_manager() {
  return cachebank::ResourceManager::global_manager();
};

#ifdef __cplusplus
}
#endif
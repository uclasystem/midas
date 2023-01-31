#include "resource_manager.h"
#include "../../inc/resource_manager.hpp"

#ifdef __cplusplus
extern "C" {
#endif

ResourceManager get_global_manager() {
  return cachebank::ResourceManager::global_manager();
};

#ifdef __cplusplus
}
#endif
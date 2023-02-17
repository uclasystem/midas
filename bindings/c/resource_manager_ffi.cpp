#include "resource_manager.h"
#include "../../inc/resource_manager.hpp"

ResourceManager get_global_manager(void) {
  return cachebank::ResourceManager::global_manager();
};

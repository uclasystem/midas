#include "resource_manager.h"
#include "../../inc/resource_manager.hpp"

resource_manager_t midas_get_global_manager(void) {
  return midas::ResourceManager::global_manager();
};

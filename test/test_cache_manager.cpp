#include <atomic>
#include <cstring>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "cache_manager.hpp"
#include "log.hpp"
#include "object.hpp"
#include "resource_manager.hpp"

constexpr static int kNumPools = 100;

int main() {
  auto resource_manager = cachebank::ResourceManager::global_manager();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000)); // to establish connection
  auto cache_manager = cachebank::CacheManager::global_cache_manager();
  for (int i = 0; i < kNumPools; i++) {
    std::string pool_name = "test" + std::to_string(i);
    cache_manager->create_pool(pool_name);
    std::cout << "Num pools: " << cache_manager->num_pools() << std::endl;
  }

  for (int i = 0; i < kNumPools; i++) {
    std::string pool_name = "test" + std::to_string(i);
    cache_manager->delete_pool(pool_name);
    std::cout << "Num pools: " << cache_manager->num_pools() << std::endl;
  }

  return 0;
}
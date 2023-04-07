#include <iostream>

#include "constants.hpp"
#include "feat_extractor.hpp"
#include "utils.hpp"

// [midas]
#include "cache_manager.hpp"

namespace hdsearch {
float cache_ratio = 1.0;
size_t cache_size = (kSimulate ? kSimuNumImgs : 41620ull) * (80 + 8192);
} // namespace hdsearch

using namespace hdsearch;

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::stof(argv[1]);
  midas::CacheManager::global_cache_manager()->create_pool(cachepool_name);
  auto pool =
      midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
  pool->update_limit(cache_size * cache_ratio);

  FeatExtractor client;
  if (kSimulate)
    client.simu_warmup_cache();
  else
    client.warmup_cache();
  client.perf();
  std::cout << "Test passed!" << std::endl;
  return 0;
}
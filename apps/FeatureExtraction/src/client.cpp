#include <iostream>

#include "constants.hpp"
#include "feat_extractor.hpp"
#include "redis_utils.hpp"

float cache_ratio = 1.0;
constexpr size_t cache_size = 420 * 1024 * 1024;

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cerr << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::atof(argv[1]);

  if (FeatExt::kUseRedis) {
    auto redis = global_redis();
    // std::cout << redis.ping() << std::endl;
    redis->command("config", "set", "maxmemory",
                  static_cast<int>(cache_size * cache_ratio));
  }

  FeatExt::FeatExtractor client;
  client.warmup_cache(cache_ratio);
  client.perf();

  return 0;
}
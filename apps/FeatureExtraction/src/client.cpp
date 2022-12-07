#include <iostream>

#include "constants.hpp"
#include "feat_extractor.hpp"
#include "redis_utils.hpp"

float cache_ratio = 1.0;
constexpr size_t cache_size = 420 * 1024 * 1024;

int main(int argc, char *argv[]) {
  auto redis = global_redis();
  // std::cout << redis.ping() << std::endl;

  cache_ratio = std::atof(argv[1]);
  redis->command("config", "set", "maxmemory",
                static_cast<int>(cache_size * cache_ratio));

  FeatExt::FeatExtractor client;

  // MD5 value of the first image
  auto val = redis->get("F5E98381292CDB1233BC9CF072197C83");
  if (val) {
    // std::cout << val->length() << " " << std::setw(2) << *val << std::endl;
    std::cout << val->length() << std::endl;
  } else {
    client.warmup_redis(cache_ratio);
  }

  // init_inference_sockets();
  // initInfClient("localhost", "10080");

  client.perf();

  return 0;
}
#include <iostream>

#include "constants.hpp"
#include "feat_extractor.hpp"

float cache_ratio = 1.0;
constexpr size_t cache_size = 420 * 1024 * 1024;

int main(int argc, char *argv[]) {
  Redis redis = init_redis_client_pool();
  // std::cout << redis.ping() << std::endl;

  cache_ratio = std::atof(argv[1]);
  redis.command("config", "set", "maxmemory",
                static_cast<int>(cache_size * cache_ratio));

  FeatExt::FeatExtractor client(redis, "val_img_names.txt", "enb5_feat_vec.data");
  // gen_fake_feats(41620);

  auto val = redis.get("F5E98381292CDB1233BC9CF072197C83");
  if (val) {
    // std::cout << val->length() << " " << std::setw(2) << *val << std::endl;
    std::cout << val->length() << std::endl;
  } else {
    client.warmup_redis();
  }

  // init_inference_sockets();
  // initInfClient("localhost", "10080");

  client.perf();

  return 0;
}
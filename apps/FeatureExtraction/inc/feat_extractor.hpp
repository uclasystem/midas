#pragma once

#include <random>

#include "feat_extractor.hpp"
#include "redis_utils.hpp"
#include "utils.hpp"
#include "zipf.hpp"

namespace FeatExt {
struct FeatReq {
  int tid;
  StringView *feat;
  std::string filename;
};

class FeatExtractionPerf {
public:
  FeatExtractionPerf(Redis &_redis, const std::string &img_file_name,
                     const std::string &feat_file_name);
  ~FeatExtractionPerf();
  int warmup_redis();
  void perf();

private:
  int load_imgs(const std::string &img_file_name);
  int load_feats(const std::string &feat_file_name);

  FeatReq gen_req(int tid);
  bool serve_req(FeatReq img_req);

  Redis &redis;
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<StringView> feats;

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_uniform;
  zipf_table_distribution<> dist_zipf;
};
} // namespace FeatExt
#pragma once

#include <random>
#include <vector>

#include "constants.hpp"
#include "redis_utils.hpp"
#include "utils.hpp"

namespace FeatExt {
struct FeatReq {
  int tid;
  std::string filename;
  Feature *feat;
};

class FeatExtractor {
public:
  FeatExtractor();
  ~FeatExtractor();
  int warmup_cache(float cache_ratio = 1.0);
  void perf();

private:
  int load_imgs(const std::string &img_file_name);
  int load_feats(const std::string &feat_file_name);

  void gen_load();
  bool serve_req(FeatReq img_req);

  std::vector<FeatReq> reqs[kNrThd];
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<Feature *> feats;
};
} // namespace FeatExt
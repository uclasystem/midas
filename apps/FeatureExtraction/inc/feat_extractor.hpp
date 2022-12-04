#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <ratio>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <thread>

#include "feat_extractor.hpp"
#include "perf.hpp"
#include "redis_utils.hpp"
#include "utils.hpp"
#include "zipf.hpp"

namespace FeatExt {
struct FeatReq {
  int tid;
  StringView *feat;
  std::string filename;
};

class FeatExtractionPerf : PerfAdapter {
public:
  FeatExtractionPerf(Redis &_redis, const std::string &img_file_name,
                     const std::string &feat_file_name);
  ~FeatExtractionPerf();
  int warmup_redis();
  void perf();

private:
  int load_imgs(const std::string &img_file_name);
  int load_feats(const std::string &feat_file_name);

  FeatReq gen_uniform_req(int tid);
  FeatReq gen_skewed_req(int tid);

  bool serve_req(FeatReq img_req);

  Redis &redis;
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<StringView> feats;

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_0_maxnrimgs;
  zipf_table_distribution<> dist_zipf;
};
} // namespace FeatExt
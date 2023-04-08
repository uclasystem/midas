#pragma once

#include <memory>
#include <random>
#include <vector>

#include "fake_backend.hpp"
#include "utils.hpp"

// [midas]
#include "cache_manager.hpp"
#include "perf.hpp"
#include "sync_hashmap.hpp"
#include "zipf.hpp"

namespace hdsearch {

struct FeatReq : midas::PerfRequest {
  int tid;
  int rid;
  std::string filename;
  Feature *feat;
};

class FeatExtractor : public midas::PerfAdapter {
public:
  FeatExtractor();
  ~FeatExtractor();
  std::unique_ptr<midas::PerfRequest> gen_req(int tid) override;
  bool serve_req(int tid, const midas::PerfRequest *img_req) override;

private:
  size_t load_imgs();
  size_t load_feats();

  // To re-construct cache-miss objects
  int construct_callback(void *arg);

  FakeBackend fakeGPUBackend;

  size_t nr_imgs;
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<std::shared_ptr<Feature>> feats;

  std::unique_ptr<midas::zipf_table_distribution<>> zipf_dist;
  std::unique_ptr<std::uniform_int_distribution<>> uni_dist;
  std::shared_ptr<std::mt19937> gens[kNrThd];

  struct {
    int nr_hit = 0;
    int nr_miss = 0;
  } perthd_cnts[kNrThd];
  void report_hit_rate();

  // midas cache
  midas::CachePool *cpool;
  std::shared_ptr<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>> feat_map;
};
} // namespace hdsearch
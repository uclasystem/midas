#pragma once

#include <random>
#include <vector>

#include "fake_backend.hpp"
#include "utils.hpp"

// [midas]
#include "cache_manager.hpp"
#include "sync_hashmap.hpp"

namespace hdsearch {
class FeatExtractor {
public:
  FeatExtractor();
  ~FeatExtractor();
  int warmup_cache();
  int simu_warmup_cache();
  std::vector<Trace> perf(uint64_t miss_ddl_us = 10ul * 1000 * 1000); // 10s
  void gen_load();
  std::vector<Trace> get_timeseries_nth_lats(uint64_t interval_us, double nth);

private:
  size_t load_imgs();
  size_t load_feats();

  bool serve_req(const FeatReq &img_req);
  // To re-construct cache-miss objects
  int construct_callback(void *arg);

  FakeBackend fakeGPUBackend;

  size_t nr_imgs;
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<std::shared_ptr<Feature>> feats;

  struct {
    int nr_hit = 0;
    int nr_miss = 0;
  } perthd_cnts[kNrThd];
  void report_hit_rate();
  std::vector<FeatReq> reqs[kNrThd];
  std::shared_ptr<std::mt19937> gens[kNrThd];

  midas::CachePool *cpool;
  std::shared_ptr<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>> feat_map;
};
} // namespace hdsearch
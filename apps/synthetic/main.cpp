#include <chrono>
#include <iostream>
#include <memory>
#include <random>
#include <thread>
#include <vector>

// [Midas]
#include "cache_manager.hpp"
#include "perf.hpp"
#include "time.hpp"
#include "utils.hpp"
#include "zipf.hpp"
#include "sync_kv.hpp"

namespace synthetic {

constexpr static int kNumBuckets = 1 << 20;
constexpr static float kSkewness = 0.99;
constexpr static int kNumThds = 12;
constexpr static int kComputeCost = 30;
constexpr static int64_t kVSize = 4000;
constexpr static int64_t kNumValues = 2 * 1024 * 1024;
constexpr static int64_t kCacheSize = (sizeof(int) + kVSize) * kNumValues * 1.1;
constexpr static float kCacheRatio = 0.2;

struct Value {
  char data[kVSize];
};

struct Req : public midas::PerfRequest {
  int tid;
  int key;
};

class App : public midas::PerfAdapter {
public:
  App(int recon_cost_ = 16) : recon_cost(recon_cost_) {
    auto cmanager = midas::CacheManager::global_cache_manager();
    cmanager->create_pool("synthetic");
    pool = cmanager->get_pool("synthetic");
    assert(pool);
    pool->update_limit(kCacheRatio * kCacheSize);
    cache = std::make_unique<midas::SyncKV<kNumBuckets>>(pool);

    for (int i = 0; i < kNumThds; i++) {
      std::random_device rd;
      gens[i] = std::make_unique<std::mt19937>(rd());
      zipf_dists[i] = std::make_unique<midas::zipf_table_distribution<>>(
          kNumValues, kSkewness);
    }
  }

  std::unique_ptr<midas::PerfRequest> gen_req(int tid) override {
    int key = (*zipf_dists[tid])(*gens[tid]);

    auto req = std::make_unique<Req>();
    req->tid = tid;
    req->key = key;

    return req;
  }

  bool serve_req(int tid, const midas::PerfRequest *req_) override {
    auto *req = dynamic_cast<const Req *>(req_);
    compute(req->key);
    return true;
  }

  void compute(int key) {
    auto v = cache->get<int, Value>(key);
    if (!v) {
      reconstruct(key);
    }
    std::this_thread::sleep_for(std::chrono::microseconds(kComputeCost));
  }

  void reconstruct(int key) {
    auto stt = midas::Time::get_cycles_stt();
    // std::this_thread::sleep_for(std::chrono::microseconds(recon_cost));
    // usleep(recon_cost);
    while (true) {
      volatile int tmp = 0;
      for (int i = 0; i < 50 * recon_cost; i++)
        tmp++;
      auto ts = midas::Time::get_cycles_end();
      if ((ts - stt) >= recon_cost * midas::kCPUFreq)
        break;
    }
    Value v;
    cache->set<int, Value>(key, v);
    auto end = midas::Time::get_cycles_end();
    pool->record_miss_penalty(end - stt, midas::kPageSize);
  }

  void warmup() {
    std::cout << "Start warm up..." << std::endl;
    for (int i = 0; i < kNumValues; i++) {
      Value v;
      cache->set<int, Value>(i, v);
    }
    std::cout << "Warm up done!" << std::endl;
  }

  int recon_cost;
  midas::CachePool *pool;
  std::unique_ptr<midas::SyncKV<kNumBuckets>> cache;

  std::unique_ptr<std::mt19937> gens[kNumThds];
  std::unique_ptr<midas::zipf_table_distribution<>> zipf_dists[kNumThds];
};
} // namespace synthetic


int main() {
  std::vector<int> recon_costs = {0,  1,   2,   4,   8,    16,   32,
                                  64, 128, 256, 512, 1024, 2048, 4096};
  synthetic::App app;
  app.warmup();
  for (auto recon_cost : recon_costs) {
    app.recon_cost = recon_cost;
    midas::Perf perf(app);
    auto target_kops = 200;
    auto duration_us = 20ull * midas::to_us;
    auto warmup_us = 0ull * midas::to_us;
    auto miss_ddl = 1ull * midas::to_us;
    perf.run(synthetic::kNumThds, target_kops, duration_us, warmup_us, miss_ddl);
    std::cout << "Real Tput: " << perf.get_real_kops() << " Kops" << std::endl;
    // std::cout << "P99 Latency: " << perf.get_nth_lat(99) << " us" << std::endl;
  }
}
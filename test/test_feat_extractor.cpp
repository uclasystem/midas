#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <openssl/md5.h>
#include <random>
#include <ratio>
#include <string>
#include <string_view>
#include <thread>

#include "cache_manager.hpp"
#include "sync_hashmap.hpp"
#include "time.hpp"
#include "zipf.hpp"

constexpr static int kFeatDim = 2048;
constexpr static int kMD5Len = 32;

constexpr static int kMissPenalty = 1; // ms
constexpr static int kNrThd = 24;
constexpr static int KPerThdLoad = 100000;
constexpr static int kNumBuckets = 1 << 20;

constexpr static bool kSkewedDist = true; // false for uniform distribution
constexpr static double kSkewness = 0.9;  // zipf

constexpr static bool kSimulate = true;
constexpr static int kSimuNumImgs = 1000 * 1000;

constexpr static int kStatInterval = 2; // seconds

const static std::string data_dir =
    "/mnt/ssd/yifan/code/cachebank/apps/FeatureExtraction/data/";
const static std::string md5_filename = data_dir + "md5.txt";
const static std::string img_filename = data_dir + "val_img_names.txt";
const static std::string feat_filename = data_dir + "enb5_feat_vec.data";

const static std::string cachepool_name = "feats";
float cache_ratio = 1.0;
size_t cache_size = (kSimulate ? kSimuNumImgs : 41620ull) * (80 + 8192);

struct MD5Key {
  char data[kMD5Len];

  const std::string to_string() {
    char str[kMD5Len + 1];
    std::memcpy(str, data, kMD5Len);
    str[kMD5Len] = '\0';
    return str;
  }
};

struct Feature {
  float data[kFeatDim];
};
static_assert(sizeof(Feature) == sizeof(float) * kFeatDim,
              "Feature struct size incorrect");

inline void md5_from_file(MD5Key &md5_result, const std::string &filename) {
  std::ifstream file(filename, std::ifstream::binary);
  MD5_CTX md5Context;
  MD5_Init(&md5Context);
  char buf[1024 * 16];
  while (file.good()) {
    file.read(buf, sizeof(buf));
    MD5_Update(&md5Context, buf, file.gcount());
  }
  unsigned char result[MD5_DIGEST_LENGTH];
  MD5_Final(result, &md5Context);

  std::stringstream md5str_stream;
  md5str_stream << std::hex << std::uppercase << std::setfill('0');
  for (const auto &byte : result)
    md5str_stream << std::setw(2) << (int)byte;
  md5str_stream << "\0";
  std::memcpy(md5_result.data, md5str_stream.str().c_str(), kMD5Len);
}

namespace std {
template <> struct hash<MD5Key> {
  size_t operator()(const MD5Key &k) const {
    return std::hash<std::string_view>()(std::string_view(k.data, kMD5Len));
  }
};

template <> struct equal_to<MD5Key> {
  size_t operator()(const MD5Key &k1, const MD5Key &k2) const {
    return std::memcmp(k1.data, k2.data, kMD5Len) == 0;
  }
};
} // namespace std

class FakeBackend {
public:
  FakeBackend() : _arrival_req_id(-1), _processed_req_id(-1), _alive(true) {
    int kProcessors = 2;
    for (int i = 0; i < kProcessors; i++) {
      processor_thds.push_back(std::thread([&]() { processor(); }));
    }
  }
  ~FakeBackend() {
    _alive = false;
    {
      std::unique_lock<std::mutex> lk(_p_mtx);
      _p_cv.notify_all();
    }
    for (auto &thd : processor_thds)
      thd.join();
  }
  Feature *serve_req() {
    int req_id = 0;
    {
      std::unique_lock<std::mutex> plk(_p_mtx);
      req_id = _arrival_req_id.fetch_add(1) + 1;
    }
    _p_cv.notify_all();
    while (_processed_req_id.load() < req_id)
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    auto ret = new Feature();
    return ret;
  }

private:
  int processor() {
    while (_alive) {
      std::unique_lock<std::mutex> plk(_p_mtx);
      _p_cv.wait(plk, [&] {
        return !_alive || _arrival_req_id.load() > _processed_req_id.load();
      });

      while (_arrival_req_id.load() > _processed_req_id.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kMissPenalty));
        _processed_req_id.fetch_add(1);
      }
    }
    return 0;
  }
  std::mutex _p_mtx;
  std::condition_variable _p_cv;

  std::atomic<int> _arrival_req_id;
  std::atomic<int> _processed_req_id;

  bool _alive;
  std::vector<std::thread> processor_thds;
};

struct FeatReq {
  int tid;
  int rid;
  std::string filename;
  Feature *feat;
  uint64_t start_us;
};

struct Trace {
  uint64_t absl_start_us;
  uint64_t start_us;
  uint64_t duration;
};

class FeatExtractor {
public:
  FeatExtractor();
  ~FeatExtractor();
  int warmup_cache();
  int simu_warmup_cache();
  void perf(uint64_t miss_ddl_us = 10ul * 1000 * 1000); // 10s
  void gen_load();

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

/** initialization & utils */
FeatExtractor::FeatExtractor() : raw_feats(nullptr), nr_imgs(0) {
  cpool = midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
  if (!cpool) {
    std::cerr << "Failed to get cache pool!" << std::endl;
    exit(-1);
  }
  feat_map =
      std::make_unique<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>>(cpool);
  load_imgs();
  load_feats();
  nr_imgs = kSimulate ? kSimuNumImgs : imgs.size();

  cpool->set_construct_func(
      [&](void *args) { return construct_callback(args); });

  for (int i = 0; i < kNrThd; i++) {
    std::random_device rd;
    gens[i] = std::make_shared<std::mt19937>(rd());
  }
  // gen_load();
}

FeatExtractor::~FeatExtractor() {
  if (raw_feats)
    delete[] raw_feats;
}

int FeatExtractor::construct_callback(void *arg) {
  auto args_ = reinterpret_cast<midas::ConstructArgs *>(arg);
  auto md5 = reinterpret_cast<const MD5Key *>(args_->key);
  auto feat_buf = reinterpret_cast<Feature *>(args_->value);
  assert(args_->key_len == sizeof(MD5Key));

  auto feat = fakeGPUBackend.serve_req();
  if (feat_buf) {
    assert(args_->value_len == sizeof(Feature));
    std::memcpy(feat_buf, feat, sizeof(Feature));
    delete feat;
  } else {
    args_->value = feat;
    args_->value_len = sizeof(Feature);
  }

  return 0;
}

void FeatExtractor::gen_load() {
  midas::zipf_table_distribution<> zipf_dist(nr_imgs, kSkewness);
  std::uniform_int_distribution<> uni_dist(0, nr_imgs - 1);

  int nr_tests = 10;
  std::vector<double> target_kopss;
  std::vector<uint64_t> durations;
  for (int i = 0; i < nr_tests; i++) {
    target_kopss.emplace_back(i + 1);
    durations.emplace_back(10); // seconds
  }
  const uint64_t us = 1000 * 1000;
  uint64_t transit_dur = 10; // seconds
  int transit_stages = 10;
  uint64_t stage_us = transit_dur * us / transit_stages;

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    thds.emplace_back([&, tid = tid] {
      reqs[tid].clear();
      uint64_t cur_us = 0;
      for (int i = 0; i < nr_tests; i++) {
        auto target_kops = target_kopss[i];
        auto duration_us = cur_us + durations[i] * us; // seconds -> us
        std::exponential_distribution<double> ed(target_kops / 1000 / kNrThd);
        while (cur_us < duration_us) {
          auto interval = std::max(1l, std::lround(ed(*gens[tid])));
          int id = kSkewedDist ? zipf_dist(*gens[tid]) : uni_dist(*gens[tid]);
          id = nr_imgs - 1 - id;
          FeatReq req{
            .tid = tid, .rid = id, .filename = imgs.at(id % imgs.size()),
            .feat = feats.at(id % feats.size()).get(),
            .start_us = cur_us};
          reqs[tid].emplace_back(req);
          cur_us += interval;
        }
        if (i == nr_tests - 1) // transition
          break;
        const auto transit_kops_step =
            (target_kopss[i + 1] - target_kopss[i]) / transit_stages;
        for (int j = 0; j < transit_stages; j++) {
          auto transit_kops = target_kops + transit_kops_step * j;
          std::exponential_distribution<double> ed(transit_kops / 1000 /
                                                   kNrThd);
          auto duration_us = cur_us + stage_us;
          while (cur_us < duration_us) {
            auto interval = std::max(1l, std::lround(ed(*gens[tid])));
            int id = kSkewedDist ? zipf_dist(*gens[tid]) : uni_dist(*gens[tid]);
            id = nr_imgs - 1 - id;
            FeatReq req{.tid = tid,
                        .rid = id,
                        .filename = imgs.at(id % imgs.size()),
                        .feat = feats.at(id % feats.size()).get(),
                        .start_us = cur_us};
            reqs[tid].emplace_back(req);
            cur_us += interval;
          }
        }
      }
    });
  }
  for (auto &thd : thds)
    thd.join();
  std::cout << "Finish load generation." << std::endl;
}

bool FeatExtractor::serve_req(const FeatReq &req) {
  MD5Key md5;
  md5_from_file(md5, req.filename);
  if (kSimulate) {
    std::ostringstream oss;
    oss << std::setw(32) << std::setfill('0') << req.rid;
    std::string md5_str = oss.str();
    md5_str.copy(md5.data, kMD5Len);
  }
  auto feat_opt = feat_map->get(md5);
  if (feat_opt) {
    perthd_cnts[req.tid].nr_hit++;
    return true;
  }

  // Cache miss
  auto stt = midas::Time::get_cycles_stt();
  perthd_cnts[req.tid].nr_miss++;
  midas::ConstructArgs args{.key = &md5,
                           .key_len = sizeof(md5),
                           .value = req.feat,
                           .value_len = sizeof(Feature)};
  assert(cpool->construct(&args) == 0);
  feat_map->set(md5, req.feat);
  auto end = midas::Time::get_cycles_end();

  cpool->record_miss_penalty(end - stt, sizeof(*req.feat));
  return true;
}

size_t FeatExtractor::load_imgs() {
  std::ifstream img_file(img_filename, std::ifstream::in);
  if (!img_file.good()) {
    std::cerr << "cannot open img_file " << img_filename << std::endl;
    return -1;
  }

  while (img_file.good()) {
    std::string name;
    std::getline(img_file, name);
    if (!name.empty())
      imgs.push_back(name);
    // std::cout << name << " " << imgs.at(imgs.size() - 1) << std::endl;
  }

  size_t nr_imgs = imgs.size();
  MD5Key md5;
  md5_from_file(md5, imgs[0]);
  std::cout << "Load " << nr_imgs << " images, MD5 of " << imgs[0] << ": "
            << md5.to_string() << std::endl;

  return nr_imgs;
}

size_t FeatExtractor::load_feats() {
  size_t nr_imgs = imgs.size();
  raw_feats = new char[nr_imgs * kFeatDim * sizeof(float)];
  size_t nr_feat_vecs = 0;

  std::ifstream feat_file(feat_filename, std::ifstream::binary);
  if (feat_file.good()) {
    feat_file.read(raw_feats, nr_imgs * sizeof(float) * kFeatDim);
  }

  char *ptr = raw_feats;
  for (int i = 0; i < nr_imgs; i++) {
    auto new_feat = std::make_shared<Feature>();
    feats.emplace_back(new_feat);
    std::memcpy(new_feat->data, ptr, sizeof(float) * kFeatDim);
    ptr += sizeof(float) * kFeatDim;
  }

  return feats.size();
}

int FeatExtractor::warmup_cache() {
  std::ifstream md5_file(md5_filename);

  size_t nr_imgs = imgs.size();
  std::cout << nr_imgs << " " << feats.size() << std::endl;
  // for (int i = 0; i < nr_imgs * cache_ratio; i++) {
  for (int i = 0; i < nr_imgs; i++) {
    MD5Key md5;
    std::string md5_str;
    md5_file >> md5_str;
    md5_str.copy(md5.data, kMD5Len);

    // md5_from_file(md5, imgs.at(i));
    // std::cout << imgs.at(i) << " " << md5 << std::endl;
    feat_map->set(md5, *feats[i]);
  }
  std::cout << "Done warm up cache" << std::endl;
  return 0;
}

int FeatExtractor::simu_warmup_cache() {
  std::cout << "Warming up cache with synthetic data..." << std::endl;
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    thds.push_back(std::thread([&, tid = tid] {
      const auto chunk = (nr_imgs + kNrThd - 1) / kNrThd;
      auto stt = chunk * tid;
      auto end = std::min(stt + chunk, nr_imgs);
      for (int i = stt; i < end; i++) {
        MD5Key md5;
        std::ostringstream oss;
        oss << std::setw(32) << std::setfill('0') << i;
        std::string md5_str = oss.str();
        md5_str.copy(md5.data, kMD5Len);

        feat_map->set(md5, *feats[i % feats.size()]);
      }
    }));
  }
  for (auto &thd : thds)
    thd.join();
  std::cout << "Done warm up cache" << std::endl;
  return 0;
}

void FeatExtractor::perf(uint64_t miss_ddl_us) {
  gen_load();

  std::atomic_int_fast32_t nr_succ{0};
  bool stop = false;
  auto stt = std::chrono::high_resolution_clock::now();
  std::thread perf_thd([&] {
    while (!stop) {
      std::this_thread::sleep_for(std::chrono::seconds(kStatInterval));
      std::cout << "Tput " << nr_succ / 1000.0 / kStatInterval << " Kops"
                << std::endl;
      nr_succ = 0;
    }
  });

  std::vector<Trace> all_traces[kNrThd];
  std::vector<std::thread> worker_thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    worker_thds.emplace_back(std::thread([&, tid = tid] {
      auto start_us = midas::Time::get_us_stt();
      auto &thd_reqs = reqs[tid];
      auto &thd_traces = all_traces[tid];
      int cnt = 0;
      for (auto &req : thd_reqs) {
        auto relative_us = midas::Time::get_us_stt() - start_us;
        if (req.start_us > relative_us) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(req.start_us - relative_us));
        } else if (req.start_us + miss_ddl_us < relative_us) {
          continue;
        }
        Trace trace;
        trace.absl_start_us = midas::Time::get_us_stt();
        trace.start_us = trace.absl_start_us - start_us;
        serve_req(req);
        trace.duration =
            midas::Time::get_us_stt() - start_us - trace.start_us;
        thd_traces.emplace_back(trace);
        cnt++;
        if (cnt % 100 == 0) {
          nr_succ += 100;
          cnt = 0;
        }
      }
    }));
  }

  for (auto &thd : worker_thds) {
    thd.join();
  }
  stop = true;
  perf_thd.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - stt).count();
  auto tput = static_cast<float>(kNrThd * KPerThdLoad) / duration;
  std::cout << "Perf done. Duration: " << duration
            << " ms, Throughput: " << tput << " Kops" << std::endl;
  report_hit_rate();

  std::vector<Trace> gathered_traces;
  for (int i = 0; i < kNrThd; i++) {
    gathered_traces.insert(gathered_traces.end(), all_traces[i].begin(),
                           all_traces[i].end());
  }
  std::cout << gathered_traces.size() << std::endl;
}

void FeatExtractor::report_hit_rate() {
  int nr_hit = 0;
  int nr_miss = 0;
  for (int i = 0; i < kNrThd; i++) {
    nr_hit += perthd_cnts[i].nr_hit;
    nr_miss += perthd_cnts[i].nr_miss;
  }
  std::cout << "Cache hit ratio = " << nr_hit << "/" << nr_hit + nr_miss
            << " = " << 1.0 * nr_hit / (nr_hit + nr_miss) << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::stof(argv[1]);
  midas::CacheManager::global_cache_manager()->create_pool(cachepool_name);
  auto pool = midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
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
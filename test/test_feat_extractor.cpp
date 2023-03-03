#include <condition_variable>
#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <openssl/md5.h>
#include <random>
#include <string>
#include <string_view>
#include <thread>

#include "cache_manager.hpp"
#include "resource_manager.hpp"
#include "sync_hashmap.hpp"
#include "time.hpp"
#include "zipf.hpp"

constexpr static int kFeatDim = 2048;
constexpr static int kMD5Len = 32;

constexpr static int kMissPenalty = 1; // ms
constexpr static int kNrThd = 24;
constexpr static int KPerThdLoad = 10000;
constexpr static int kNumBuckets = 1 << 20;

constexpr static bool kSkewedDist = true; // false for uniform distribution
constexpr static double kSkewness = 0.9;  // zipf

constexpr static bool kSimulate = true;
constexpr static int kSimuNumImgs = 1000 * 1000;

const static std::string data_dir =
    "/mnt/ssd/yifan/code/cachebank/apps/FeatureExtraction/data/";
const static std::string md5_filename = data_dir + "md5.txt";
const static std::string img_filename = data_dir + "val_img_names.txt";
const static std::string feat_filename = data_dir + "enb5_feat_vec.data";

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
  void serve_req() {
    int req_id = 0;
    {
      std::unique_lock<std::mutex> plk(_p_mtx);
      req_id = _arrival_req_id.fetch_add(1) + 1;
    }
    _p_cv.notify_all();
    while (_processed_req_id.load() < req_id)
      std::this_thread::sleep_for(std::chrono::microseconds(100));
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
};

class FeatExtractor {
public:
  FeatExtractor();
  ~FeatExtractor();
  int warmup_cache();
  int simu_warmup_cache();
  void perf();

private:
  size_t load_imgs();
  size_t load_feats();

  void gen_load();
  bool serve_req(const FeatReq &img_req);

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

  std::shared_ptr<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>>
      feat_map;

  friend int construct_callback(void *arg);
};

struct ConstructArgs {
  MD5Key *key;
  FeatExtractor *extractor;
  int tid;

  Feature *ret;
};

int construct_callback(void *arg) {
  auto args_ = reinterpret_cast<ConstructArgs *>(arg);
  auto extractor = args_->extractor;
  auto md5 = args_->key;
  auto feat = args_->ret;
  auto tid = args_->tid;

  extractor->perthd_cnts[tid].nr_miss++;
  extractor->fakeGPUBackend.serve_req();
  extractor->feat_map->set(*md5, *feat);

  return 0;
}

/** initialization & utils */
FeatExtractor::FeatExtractor() : raw_feats(nullptr), nr_imgs(0) {
  feat_map =
      std::make_unique<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>>();
  load_imgs();
  load_feats();
  nr_imgs = kSimulate ? kSimuNumImgs : imgs.size();

  auto cpool = midas::CachePool::global_cache_pool();
  cpool->set_construct_func(construct_callback);

  for (int i = 0; i < kNrThd; i++) {
    std::random_device rd;
    gens[i] = std::make_shared<std::mt19937>(rd());
  }
  gen_load();
}

FeatExtractor::~FeatExtractor() {
  if (raw_feats)
    delete[] raw_feats;
}

void FeatExtractor::gen_load() {
  midas::zipf_table_distribution<> zipf_dist(nr_imgs, kSkewness);
  std::uniform_int_distribution<> uni_dist(0, nr_imgs - 1);

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    reqs[tid].clear();
    for (int o = 0; o < KPerThdLoad; o++) {
      int id = kSkewedDist ? zipf_dist(*gens[tid]) : uni_dist(*gens[tid]);
      id = nr_imgs - 1 - id;
      FeatReq req{.tid = tid,
                  .rid = id,
                  .filename = imgs.at(id % imgs.size()),
                  .feat = feats.at(id % feats.size()).get()};
      reqs[tid].push_back(req);
    }
  }
  std::cout << "Finish load generation." << std::endl;
}

bool FeatExtractor::serve_req(const FeatReq &req) {
  auto cpool = midas::CachePool::global_cache_pool();

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
  // perthd_cnts[req.tid].nr_miss++;
  // fakeGPUBackend.serve_req();
  // feat_map->set(md5, *req.feat);
  ConstructArgs arg{
      .key = &md5, .extractor = this, .tid = req.tid, .ret = req.feat};
  assert(cpool->construct(&arg) == 0);
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

void FeatExtractor::perf() {
  auto stt = std::chrono::high_resolution_clock::now();
  std::vector<std::thread> worker_thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    worker_thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < KPerThdLoad; i++) {
        // auto req = gen_req(tid);
        serve_req(reqs[tid][i]);
      }
    }));
  }

  for (auto &thd : worker_thds) {
    thd.join();
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end - stt).count();
  auto tput = static_cast<float>(kNrThd * KPerThdLoad) / duration;
  std::cout << "Perf done. Duration: " << duration
            << " ms, Throughput: " << tput << " Kops" << std::endl;
  report_hit_rate();
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

  midas::ResourceManager::global_manager()->UpdateLimit(cache_size *
                                                            cache_ratio);

  FeatExtractor client;
  if (kSimulate)
    client.simu_warmup_cache();
  else
    client.warmup_cache();
  client.perf();
  std::cout << "Test passed!" << std::endl;
  return 0;
}
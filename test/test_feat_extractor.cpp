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
#include <condition_variable>

#include "sync_hashmap.hpp"
#include "zipf.hpp"

constexpr static int kFeatDim = 2048;
// constexpr static int MD5_DIGEST_LENGTH = 16;
constexpr static int kMD5Len = MD5_DIGEST_LENGTH;

constexpr static int kMissPenalty = 12; // ms
constexpr static int kNrThd = 4;
constexpr static int KPerThdLoad = 10000;
constexpr static int kNumBuckets = 1 << 20;

constexpr static bool kSkewedDist = false; // false for uniform distribution
constexpr static double kSkewness = 0.5;  // zipf

constexpr static bool kSimulate = true;

float cache_ratio = 1.0;
int cache_size = 420 * 1024 * 1024;


struct MD5Key {
  char data[kMD5Len];

  const std::string to_string() {
    std::stringstream md5str_stream;
    md5str_stream << std::hex << std::uppercase << std::setfill('0');
    for (const auto &byte : data)
      md5str_stream << std::setw(2) << (int)byte;
    md5str_stream << "\0";
    return md5str_stream.str();
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
  MD5_Final(reinterpret_cast<unsigned char *>(md5_result.data), &md5Context);
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
  std::string filename;
  Feature *feat;
};


class FeatExtractor {
public:
  FeatExtractor(const std::string &img_file_name,
                const std::string &feat_file_name);
  ~FeatExtractor();
  int warmup_cache();
  void perf();

private:
  int load_imgs(const std::string &img_file_name);
  int load_feats(const std::string &feat_file_name);

  FeatReq gen_req(int tid);
  bool serve_req(FeatReq img_req);

  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<std::shared_ptr<Feature>> feats;

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_uniform;
  cachebank::zipf_table_distribution<> dist_zipf;

  std::unique_ptr<cachebank::SyncHashMap<kNumBuckets, MD5Key, Feature>>
      feat_map;
};

/** initialization & utils */
FeatExtractor::FeatExtractor(const std::string &img_file_name,
                             const std::string &feat_file_name)
    : rd(), gen(rd()), raw_feats(nullptr), dist_zipf(0, 1) {
  feat_map =
      std::make_unique<cachebank::SyncHashMap<kNumBuckets, MD5Key, Feature>>();
  load_imgs(img_file_name);
  load_feats(feat_file_name);
}

FeatExtractor::~FeatExtractor() {
  if (raw_feats)
    delete[] raw_feats;
}

FeatReq FeatExtractor::gen_req(int tid) {
  auto id = kSkewedDist ? dist_zipf(gen) - 1 : dist_uniform(gen);
  FeatReq req{.tid = tid, .filename = imgs.at(id), .feat = feats.at(id).get()};
  return req;
}

bool FeatExtractor::serve_req(FeatReq req) {
  MD5Key md5;
  md5_from_file(md5, req.filename);
  auto feat_opt = feat_map->get(md5);
  if (feat_opt)
    return true;

  // Cache miss
  // feat_map->set(md5, *req.feat);
  if (kSimulate) {
    FakeBackend fakeGPUBackend;
    fakeGPUBackend.serve_req();
    return true;
  }
  return true;
}

int FeatExtractor::load_imgs(const std::string &img_file_name) {
  std::ifstream img_file(img_file_name, std::ifstream::in);
  if (!img_file.good()) {
    std::cerr << "cannot open img_file " << img_file_name << std::endl;
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
  std::cout << nr_imgs << " " << imgs[0] << " " << md5.to_string() << std::endl;

  dist_uniform = std::uniform_int_distribution<>(0, nr_imgs - 1);
  dist_zipf = cachebank::zipf_table_distribution<>(nr_imgs, kSkewness);

  return nr_imgs;
}

int FeatExtractor::load_feats(const std::string &feat_file_name) {
  size_t nr_imgs = imgs.size();
  raw_feats = new char[nr_imgs * kFeatDim * sizeof(float)];
  size_t nr_feat_vecs = 0;

  std::ifstream feat_file(feat_file_name, std::ifstream::binary);
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
  size_t nr_imgs = imgs.size();
  std::cout << nr_imgs << " " << feats.size() << std::endl;
  for (int i = 0; i < nr_imgs * cache_ratio; i++) {
    MD5Key md5;
    md5_from_file(md5, imgs.at(i));
    // std::cout << imgs.at(i) << " " << md5 << std::endl;
    feat_map->set(md5, *feats[i]);
  }
  std::cout << "Done warm up cache" << std::endl;
  return 0;
}

void FeatExtractor::perf() {
  auto stt = std::chrono::high_resolution_clock::now();
  std::vector<std::thread> worker_thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    worker_thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < KPerThdLoad; i++) {
        auto req = gen_req(tid);
        serve_req(req);
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
}


int main(int argc, char *argv[]) {
  if (argc < 1) {
    std::cout << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::stof(argv[1]);
  FeatExtractor client(data_dir + "val_img_names.txt",
                       data_dir + "enb5_feat_vec.data");
  client.warmup_cache();
  client.perf();
  std::cout << "Test passed!" << std::endl;
  return 0;
}
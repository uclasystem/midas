#include <chrono>
#include <random>
#include <thread>

#include "constants.hpp"
#include "fake_backend.hpp"
#include "feat_extractor.hpp"
#include "redis_utils.hpp"
#include "socket.hpp"
#include "utils.hpp"

namespace FeatExt {
using namespace sw::redis;

Socket sockets[kNrThd];

const std::string getFeatVector(Redis &redis, struct FeatReq req) {
  auto &md5 = md5_from_file(req.filename);
  auto feat_opt = redis.get(md5);
  if (feat_opt)
    return *feat_opt;

  // Cache miss
  redis.set(md5, *req.feat);
  if (kSimulate) {
    FakeBackend fakeGPUBackend;
    fakeGPUBackend.serve_req();
    return "";
  }

  sockets[req.tid].send_recv(req.filename);
  std::string feat = "";
  return feat;
}

/** initialization & utils */
FeatExtractionPerf::FeatExtractionPerf(Redis &_redis,
                                       const std::string &img_file_name,
                                       const std::string &feat_file_name)
    : redis(_redis), rd(), gen(rd()), raw_feats(nullptr), dist_zipf(0, 1) {
  load_imgs(img_file_name);
  load_feats(feat_file_name);
}

FeatExtractionPerf::~FeatExtractionPerf() {
  if (raw_feats)
    delete[] raw_feats;
}

FeatReq FeatExtractionPerf::gen_req(int tid) {
  auto id = kSkewedDist ? dist_zipf(gen) - 1 : dist_uniform(gen);
  FeatReq req{.tid = tid, .feat = &feats.at(id), .filename = imgs.at(id)};
  return req;
}

bool FeatExtractionPerf::serve_req(FeatReq img_req) {
  auto &feat = getFeatVector(redis, img_req);
  return true;
}

int FeatExtractionPerf::load_imgs(const std::string &img_file_name) {
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
  std::cout << nr_imgs << " " << imgs[0] << " " << md5_from_file(imgs[0])
            << std::endl;

  dist_uniform = std::uniform_int_distribution<>(0, nr_imgs - 1);
  dist_zipf = zipf_table_distribution<>(nr_imgs, kSkewness);

  return nr_imgs;
}

int FeatExtractionPerf::load_feats(const std::string &feat_file_name) {
  size_t nr_imgs = imgs.size();
  raw_feats = new char[nr_imgs * kFeatDim * sizeof(float)];
  size_t nr_feat_vecs = 0;

  std::ifstream feat_file(feat_file_name, std::ifstream::binary);
  if (feat_file.good()) {
    feat_file.read(raw_feats, nr_imgs * sizeof(float) * kFeatDim);
  }

  char *ptr = raw_feats;
  for (int i = 0; i < nr_imgs; i++) {
    feats.push_back(StringView(ptr, sizeof(float) * kFeatDim));
    ptr += sizeof(float) * kFeatDim;
  }

  return feats.size();
}

int FeatExtractionPerf::warmup_redis() {
  size_t nr_imgs = imgs.size();
  std::cout << nr_imgs << " " << feats.size() << std::endl;
  auto pipe = redis.pipeline(false);
  for (int i = 0; i < nr_imgs; i++) {
    auto &md5 = md5_from_file(imgs.at(i));
    // std::cout << imgs.at(i) << " " << md5 << std::endl;
    pipe.set(md5, feats.at(i));
  }
  pipe.exec();
  std::cout << "Done warm up redis" << std::endl;
  return 0;
}

void FeatExtractionPerf::perf() {
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
} // namespace FeatExt

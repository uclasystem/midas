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
#include <signal.h>
#include <sstream>
#include <string>
#include <thread>

#include "constants.hpp"
#include "fake_backend.hpp"
#include "perf.hpp"
#include "redis_utils.hpp"
#include "socket.hpp"
#include "utils.hpp"
#include "zipf.hpp"

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }
float cache_ratio = 1.0;
constexpr size_t cache_size = 420 * 1024 * 1024;

namespace FeatExt {
using namespace sw::redis;

// std::vector<Socket> sockets;
Socket sockets[kNrThd];

struct FeatReq {
  int tid;
  StringView *feat;
  std::string filename;
};

void sendReq(const std::string data) {
  static std::mutex mtx;
  std::unique_lock<std::mutex> lk(mtx);

  send_recv_socket(data.c_str(), data.length());
}

const std::string getFeatVector(Redis &redis, struct FeatReq req) {
  auto &md5 = md5_from_file(req.filename);
  auto feat_opt = redis.get(md5);
  if (feat_opt)
    return *feat_opt;

  // Cache miss
  redis.set(md5, *req.feat);
  if (kSimulate) {
    fakeGPUBackend.serve_req();
    return "";
  }

  // std::string cmd = std::string("python3 extractFeat.py ") + req.filename;
  // auto feat = ExecuteShellCommand(cmd);
  sockets[req.tid].send_recv(req.filename);
  // sendReq(req.filename);
  std::string feat = "";
  return feat;
}

class FeatExtractionPerf : PerfAdapter {
public:
  FeatExtractionPerf(Redis &_redis, const std::string &img_file_name,
                     const std::string &feat_file_name)
      : redis(_redis), rd(), gen(rd()), raw_feats(nullptr), dist_zipf(0, 1) {
    load_imgs(img_file_name);
    load_feats(feat_file_name);
  }
  ~FeatExtractionPerf() {
    if (raw_feats)
      delete[] raw_feats;
  }
  int warmup_redis();
  void perf();

private:
  int load_imgs(const std::string &img_file_name);
  int load_feats(const std::string &feat_file_name);

  FeatReq gen_uniform_req(int tid) {
    auto id = dist_0_maxnrimgs(gen);
    FeatReq req;
    req.feat = &feats.at(id);
    req.filename = imgs.at(id);
    req.tid = tid;

    return req;
  }

  FeatReq gen_skewed_req(int tid) {
    auto id = dist_zipf(gen) - 1;
    FeatReq req;
    req.feat = &feats.at(id);
    req.filename = imgs.at(id);
    req.tid = tid;

    return req;
  }

  bool serve_req(FeatReq img_req) {
    auto &feat = getFeatVector(redis, img_req);
    return true;
  }

  Redis &redis;
  std::vector<std::string> imgs;
  char *raw_feats;
  std::vector<StringView> feats;

  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> dist_0_maxnrimgs;
  zipf_table_distribution<> dist_zipf;
};

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

  dist_0_maxnrimgs = std::uniform_int_distribution<>(0, nr_imgs - 1);
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
        // const auto &req = gen_uniform_req(tid);
        const auto &req = gen_skewed_req(tid);
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


int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);

  Redis redis = init_redis_client_pool();
  // std::cout << redis.ping() << std::endl;

  cache_ratio = std::atof(argv[1]);
  redis.command("config", "set", "maxmemory",
                static_cast<int>(cache_size * cache_ratio));

  FeatExt::FeatExtractionPerf perf(redis, "val_img_names.txt", "enb5_feat_vec.data");
  // gen_fake_feats(41620);

  auto val = redis.get("F5E98381292CDB1233BC9CF072197C83");
  if (val) {
    // std::cout << val->length() << " " << std::setw(2) << *val << std::endl;
    std::cout << val->length() << std::endl;
  } else {
    perf.warmup_redis();
  }

  // init_inference_sockets();
  // initInfClient("localhost", "10080");

  perf.perf();

  return 0;
}
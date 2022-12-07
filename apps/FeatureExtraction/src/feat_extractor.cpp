#include <chrono>
#include <random>
#include <thread>

#include "constants.hpp"
#include "fake_backend.hpp"
#include "feat_extractor.hpp"
#include "redis_utils.hpp"
#include "socket.hpp"
#include "utils.hpp"
#include "zipf.hpp"

namespace FeatExt {
Socket sockets[kNrThd];

const std::string getFeatVector(struct FeatReq req) {
  auto &md5 = md5_from_file(req.filename);
  auto redis = global_redis();
  auto feat_opt = redis->get(md5);
  if (feat_opt)
    return *feat_opt;

  // Cache miss
  redis->set(md5, req.feat->to_string_view());
  if (kSimulate) {
    global_fake_backend()->serve_req();
    return "";
  }

  sockets[req.tid].send_recv(req.filename);
  return "";
}

/** initialization & utils */
FeatExtractor::FeatExtractor()
    : raw_feats(nullptr) {
  std::string img_file_name = data_dir + "val_img_names.txt";
  std::string feat_file_name = data_dir + "enb5_feat_vec.data";
  load_imgs(img_file_name);
  load_feats(feat_file_name);
  gen_load();
}

FeatExtractor::~FeatExtractor() {
  if (raw_feats)
    delete[] raw_feats;
}

void FeatExtractor::gen_load() {
  auto nr_imgs = imgs.size();
  std::random_device rd;
  std::mt19937 gen(rd());
  zipf_table_distribution<> zipf_dist(nr_imgs, kSkewness);
  std::uniform_int_distribution<> uni_dist(0, nr_imgs - 1);

  for (int tid = 0; tid < kNrThd; tid++) {
    const auto nr_imgs = imgs.size();
    reqs[tid].clear();
    for (int o = 0; o < KPerThdLoad; o++) {
      auto id = kSkewedDist ? zipf_dist(gen) : uni_dist(gen);
      FeatReq req{
          .tid = tid, .filename = imgs.at(id), .feat = feats.at(id)};
      reqs[tid].push_back(req);
    }
  }
  std::cout << "Finish load generation." << std::endl;
}

bool FeatExtractor::serve_req(FeatReq req) {
  auto &feat = getFeatVector(req);
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

  auto nr_imgs = imgs.size();
  std::cout << nr_imgs << " " << imgs[0] << " " << md5_from_file(imgs[0])
            << std::endl;
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
    feats.push_back(reinterpret_cast<Feature *>(ptr));
    ptr += sizeof(float) * kFeatDim;
  }

  return feats.size();
}

int FeatExtractor::warmup_cache(float cache_ratio) {
  const auto filename = data_dir + "md5.txt";
  std::ifstream md5_file(filename);
  if (!md5_file.is_open()) {
    std::cerr << "cannot open file " << filename << std::endl;
    return -1;
  }

  size_t nr_imgs = imgs.size();
  std::cout << nr_imgs << " " << feats.size() << std::endl;
  auto pipe = global_redis()->pipeline(false);
  for (int i = 0; i < nr_imgs * cache_ratio; i++) {
    std::string md5;
    md5_file >> md5;
    // std::cout << imgs.at(i) << " " << md5 << std::endl;
    pipe.set(md5, feats.at(i)->to_string_view());
  }
  pipe.exec();
  std::cout << "Done warm up cache" << std::endl;
  return 0;
}

void FeatExtractor::perf() {
  auto stt = std::chrono::high_resolution_clock::now();
  std::vector<std::thread> worker_thds;
  for (int tid = 0; tid < kNrThd; tid++) {
    worker_thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < KPerThdLoad; i++) {
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
}
} // namespace FeatExt

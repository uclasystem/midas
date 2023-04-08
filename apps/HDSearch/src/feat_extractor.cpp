#include <memory>
#include <random>

#include "constants.hpp"
#include "fake_backend.hpp"
#include "feat_extractor.hpp"

// [midas]
#include "perf.hpp"
#include "zipf.hpp"

namespace hdsearch {
FeatExtractor::FeatExtractor()
    : raw_feats(nullptr), nr_imgs(0), fakeGPUBackend(kNrGPUs) {
  cpool = midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
  if (!cpool) {
    std::cerr << "Failed to get cache pool!" << std::endl;
    exit(-1);
  }
  // set up midas
  cpool->set_construct_func(
      [&](void *args) { return construct_callback(args); });
  feat_map =
      std::make_unique<midas::SyncHashMap<kNumBuckets, MD5Key, Feature>>(cpool);

  // load images
  load_imgs();
  load_feats();
  nr_imgs = kSimulate ? kSimuNumImgs : imgs.size();

  // init random number generator
  zipf_dist =
      std::make_unique<midas::zipf_table_distribution<>>(nr_imgs, kSkewness);
  uni_dist = std::make_unique<std::uniform_int_distribution<>>(0, nr_imgs - 1);
  for (int i = 0; i < kNrThd; i++) {
    std::random_device rd;
    gens[i] = std::make_unique<std::mt19937>(rd());
  }
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

std::unique_ptr<midas::PerfRequest> FeatExtractor::gen_req(int tid) {
  int id = kSkewedDist ? (*zipf_dist)(*gens[tid]) : (*uni_dist)(*gens[tid]);
  id = nr_imgs - 1 - id;

  auto req = std::make_unique<FeatReq>();
  req->tid = tid;
  req->rid = id;
  req->filename = imgs.at(id % imgs.size());
  req->feat = feats.at(id % feats.size()).get();

  return req;
}

bool FeatExtractor::serve_req(int tid, const midas::PerfRequest *req_) {
  auto *req = dynamic_cast<const FeatReq *>(req_);
  MD5Key md5;
  md5_from_file(md5, req->filename);
  if (kSimulate) {
    std::ostringstream oss;
    oss << std::setw(32) << std::setfill('0') << req->rid;
    std::string md5_str = oss.str();
    md5_str.copy(md5.data, kMD5Len);
  }
  auto feat_opt = feat_map->get(md5);
  if (feat_opt) {
    perthd_cnts[req->tid].nr_hit++;
    return true;
  }

  // Cache miss
  auto stt = midas::Time::get_cycles_stt();
  perthd_cnts[req->tid].nr_miss++;
  midas::ConstructArgs args{.key = &md5,
                            .key_len = sizeof(md5),
                            .value = req->feat,
                            .value_len = sizeof(Feature)};
  assert(cpool->construct(&args) == 0);
  feat_map->set(md5, req->feat);
  auto end = midas::Time::get_cycles_end();

  cpool->record_miss_penalty(end - stt, sizeof(*req->feat));
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
} // namespace hdsearch
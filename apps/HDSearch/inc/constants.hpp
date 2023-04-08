#pragma once

#include <string>

namespace hdsearch {
constexpr static int kFeatDim = 2048;
constexpr static int kMD5Len = 32;

constexpr static int kNrThd = 24;
constexpr static int kNumBuckets = 1 << 20;

constexpr static bool kSkewedDist = true; // false for uniform distribution
constexpr static double kSkewness = 0.9;  // zipf

constexpr static bool kSimulate = true;
constexpr static int kSimuNumImgs = 1000 * 1000;

constexpr static float kMissPenalty = 1; // ms
constexpr static int kNrGPUs = 4;

const static std::string data_dir =
    "/mnt/ssd/yifan/code/cachebank/apps/FeatureExtraction/data/";
const static std::string md5_filename = data_dir + "md5.txt";
const static std::string img_filename = data_dir + "val_img_names.txt";
const static std::string feat_filename = data_dir + "enb5_feat_vec.data";

const static std::string cachepool_name = "feats";
} // namespace hdsearch
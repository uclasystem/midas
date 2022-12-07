#pragma once

#include <string>

namespace FeatExt {
constexpr static int kMD5Len = 32;
constexpr static int kFeatDim = 2048;

constexpr static int kMissPenalty = 12;  // ms
constexpr static int kNrThd = 24;
constexpr static int KPerThdLoad = 10000;

constexpr static bool kSkewedDist = true; // false for uniform distribution
constexpr static double kSkewness = 0.9;  // zipf

const static std::string data_dir = "/mnt/ssd/yifan/code/cache-service/caas/";

constexpr bool kSimulate = true;
constexpr bool kUseRedis = false;
} // namespace FeatExt

#pragma once

namespace FeatExt {
constexpr int kFeatDim = 2048;

constexpr int kMissPenalty = 12;  // ms
constexpr int kNrThd = 24;
constexpr int KPerThdLoad = 10000;

constexpr bool kSkewedDist = true; // false for uniform distribution
constexpr double kSkewness = 0.5; // zipf

constexpr bool kSimulate = false;
} // namespace FeatExt
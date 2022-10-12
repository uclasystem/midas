#pragma once

#include <chrono>

namespace cachebank {

namespace timer {
inline std::chrono::steady_clock::time_point timer() {
  return std::chrono::steady_clock::now();
}

inline double duration(const std::chrono::steady_clock::time_point &stt,
                       const std::chrono::steady_clock::time_point &end) {
  return std::chrono::duration<double>(end - stt).count();
}
} // namespace timer
} // namespace cachebank
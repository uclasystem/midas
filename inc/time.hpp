#pragma once

#include <chrono>
#include <cstdint>

#include "utils.hpp"

namespace midas {
struct Time {
public:
  static inline uint64_t get_us() { return get_us_stt(); };
  static inline uint64_t get_cycles() { return get_cycles_stt(); };
  static inline uint64_t get_us_stt();
  static inline uint64_t get_us_end();
  static inline uint64_t get_cycles_stt();
  static inline uint64_t get_cycles_end();

private:
  static inline uint64_t rdtsc();
  static inline uint64_t rdtscp();

  static inline uint64_t cycles_to_us(uint64_t cycles) noexcept;
  static inline uint64_t us_to_cycles(uint64_t us) noexcept;
};

namespace chrono_utils {
inline std::chrono::steady_clock::time_point now();
inline double duration(const std::chrono::steady_clock::time_point &stt,
                       const std::chrono::steady_clock::time_point &end);
} // namespace chrono_utils
} // namespace midas

#include "impl/time.ipp"
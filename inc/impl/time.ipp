#pragma once

namespace midas {
inline uint64_t Time::get_us_stt() { return cycles_to_us(rdtsc()); }

inline uint64_t Time::get_us_end() { return cycles_to_us(rdtscp()); }

inline uint64_t Time::get_cycles_stt() { return rdtsc(); }

inline uint64_t Time::get_cycles_end() { return rdtscp(); }

inline uint64_t Time::cycles_to_us(uint64_t cycles) noexcept {
  return cycles / kCPUFreq;
}

inline uint64_t Time::us_to_cycles(uint64_t us) noexcept {
  return us * kCPUFreq;
}

inline uint64_t Time::rdtsc() {
  uint32_t a, d;
  asm volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

inline uint64_t Time::rdtscp() {
  uint32_t a, d, c;
  asm volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

namespace chrono_utils {
inline std::chrono::steady_clock::time_point now();
inline double duration(const std::chrono::steady_clock::time_point &stt,
                       const std::chrono::steady_clock::time_point &end);

inline std::chrono::steady_clock::time_point now() {
  return std::chrono::steady_clock::now();
}

inline double duration(const std::chrono::steady_clock::time_point &stt,
                       const std::chrono::steady_clock::time_point &end) {
  return std::chrono::duration<double>(end - stt).count();
}
} // namespace chrono_utils
} // namespace midas
#ifndef __MIDAS_UTILS_H__
#define __MIDAS_UTILS_H__

#include <stdint.h>

// high resolution timer functions
static inline uint64_t rdtsc(void) {
  uint32_t a, d;
  __asm__ volatile("rdtsc" : "=a"(a), "=d"(d));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline uint64_t rdtscp(void) {
  uint32_t a, d, c;
  __asm__ volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline uint64_t get_cycles_stt(void) {
  return rdtscp();
}

static inline uint64_t get_cycles_end(void) {
  return rdtscp();
}

#endif // __MIDAS_UTILS_H__
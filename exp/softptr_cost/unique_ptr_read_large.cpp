#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <memory>
#include <numeric>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "cache_manager.hpp"
#include "object.hpp"
#include "time.hpp"

#define ACCESS_ONCE(x)                                                         \
  (*static_cast<std::remove_reference<decltype(x)>::type volatile *>(&(x)))

using data_t = uint64_t;

constexpr static int kMeasureTimes = 1'000'000; // 1M times
constexpr static uint64_t kRawMemAccessCycles = 170;
constexpr static uint64_t kCachePoolSize = 100ull * 1024 * 1024 * 1024; // 100GB

constexpr static int kNumLargeObjs = 10'000;           // 10K objs
constexpr static int kLargeObjSize = 4 * 1024 * 1024; // 4MB

struct LargeObject {
  char data[kLargeObjSize];

  LargeObject() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    constexpr static int kFillStride = 100;
    for (uint32_t i = 0; i < kLargeObjSize / kFillStride; i++) {
      data[i * kFillStride] = dist(mt);
    }
  }
};

void print_lats(std::vector<uint64_t> &durs) {
  std::sort(durs.begin(), durs.end());
  uint64_t avg = std::reduce(durs.begin(), durs.end(), 0.0) / durs.size();
  auto median = durs[durs.size() / 2];
  auto p90 = durs[durs.size() * 9 / 10];
  auto p99 = durs[durs.size() * 99 / 100];

  printf("avg\t med\t p90\t p99\n"
         "%lu\t %lu\t %lu\t %lu\n",
         avg, median, p90, p99);
}

void unique_ptr_read_large_cost() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> idx_dist(0, kNumLargeObjs - 1);
  static std::uniform_int_distribution<int> off_dist(
      0, kLargeObjSize / sizeof(data_t) - 1);

  std::vector<std::unique_ptr<LargeObject>> objs;
  for (int i = 0; i < kNumLargeObjs; i++) {
    objs.push_back(std::make_unique<LargeObject>());
  }

  uint64_t stt, end;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    auto idx = idx_dist(mt);
    auto off = off_dist(mt);

    stt = midas::Time::get_cycles_stt();
    {
      const data_t *ptr = reinterpret_cast<data_t *>(objs[idx].get()->data);
      ACCESS_ONCE(ptr[off]);
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    durs.push_back(dur_cycles);
  }
  printf("Access unique_ptr<LargeObject> latency distribution (cycles):\n");
  print_lats(durs);
}

int main(int argc, char *argv[]) {
  unique_ptr_read_large_cost();

  return 0;
}
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
constexpr static uint64_t kCachePoolSize = 100ull * 1024 * 1024 * 1024;

constexpr static int kNumSmallObjs = 1'000'000; // 1M objs
constexpr static int kSmallObjSize = 32;
constexpr static int kAlignedSize =
    64 - sizeof(midas::SmallObjectHdr); // cacheline aligned with header
static_assert(kSmallObjSize <= kAlignedSize, "AlignedSize is incorrect!");

struct SmallObject {
  char data[kAlignedSize];

  SmallObject() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < kAlignedSize; i++) {
      data[i] = dist(mt);
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

void softptr_read_small_cost() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(0, kNumSmallObjs - 1);

  auto cmanager = midas::CacheManager::global_cache_manager();
  cmanager->create_pool("small");
  auto pool = cmanager->get_pool("small");
  pool->update_limit(kCachePoolSize);
  auto allocator = pool->get_allocator();

  std::vector<midas::ObjectPtr> objs;
  for (int i = 0; i < kNumSmallObjs; i++) {
    objs.emplace_back(midas::ObjectPtr());
  }
  for (int i = 0; i < kNumSmallObjs; i++) {
    auto succ = allocator->alloc_to(kAlignedSize, &objs[i]);
    assert(succ);
  }
  for (int i = 0; i < kNumSmallObjs; i++) {
    SmallObject obj;
    auto succ = objs[i].copy_from(obj.data, kSmallObjSize);
    assert(succ);
  }

  SmallObject obj;
  uint64_t stt, end;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    auto idx = dist(mt);

    stt = midas::Time::get_cycles_stt();
    {
      objs[idx].copy_to(obj.data, kSmallObjSize);
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    if (dur_cycles > kRawMemAccessCycles)
      durs.push_back(dur_cycles);
  }
  printf("Access soft_ptr<SmallObject> latency distribution (cycles):\n");
  print_lats(durs);
}

int main(int argc, char *argv[]) {
  softptr_read_small_cost();

  return 0;
}
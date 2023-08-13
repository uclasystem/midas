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
constexpr static int kSmallObjSize = 64;

constexpr static int kNumLargeObjs = 10'000;           // 10K objs
constexpr static int kLargeObjSize = 4 * 1024 * 1024; // 4MB

struct SmallObject {
  char data[kSmallObjSize];

  SmallObject() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < kSmallObjSize; i++) {
      data[i] = dist(mt);
    }
  }
};

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

  printf("avg: %lu, median: %lu, p90: %lu, p99: %lu\n", avg, median, p90, p99);
}

void unique_ptr_read_small_cost() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(0, kNumSmallObjs - 1);

  std::vector<std::unique_ptr<SmallObject>> objs;
  for (int i = 0; i < kNumSmallObjs; i++) {
    objs.push_back(std::make_unique<SmallObject>());
  }

  uint64_t stt, end;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    auto idx = dist(mt);

    stt = midas::Time::get_cycles_stt();
    {
      const data_t *ptr = reinterpret_cast<data_t *>(objs[idx].get()->data);
      ACCESS_ONCE(*ptr);
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    durs.push_back(dur_cycles);
  }
  printf("Access unique_ptr<SmallObject>:\n");
  print_lats(durs);
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
  printf("Access unique_ptr<LargeObject>:\n");
  print_lats(durs);
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
    auto succ = allocator->alloc_to(kSmallObjSize, &objs[i]);
    assert(succ);
  }
  for (int i = 0; i < kNumSmallObjs; i++) {
    SmallObject obj;
    auto succ = objs[i].copy_from(obj.data, kSmallObjSize);
    assert(succ);
  }

  uint64_t stt, end;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    auto idx = dist(mt);

    stt = midas::Time::get_cycles_stt();
    {
      data_t dst;
      objs[idx].copy_to(&dst, sizeof(dst));
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    if (dur_cycles > kRawMemAccessCycles)
      durs.push_back(dur_cycles);
  }
  printf("Access soft_ptr<SmallObject>:\n");
  print_lats(durs);
}

void softptr_read_large_cost() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> idx_dist(0, kNumLargeObjs - 1);
  static std::uniform_int_distribution<int> off_dist(
      0, kLargeObjSize / sizeof(data_t) - 1);

  auto cmanager = midas::CacheManager::global_cache_manager();
  cmanager->create_pool("large");
  auto pool = cmanager->get_pool("large");
  pool->update_limit(kCachePoolSize);
  auto allocator = pool->get_allocator();

  std::vector<midas::ObjectPtr> objs;
  for (int i = 0; i < kNumLargeObjs; i++) {
    objs.emplace_back(midas::ObjectPtr());
  }
  for (int i = 0; i < kNumLargeObjs; i++) {
    auto succ = allocator->alloc_to(kLargeObjSize, &objs[i]);
    assert(succ);
  }
  for (int i = 0; i < kNumLargeObjs; i++) {
    LargeObject obj;
    auto succ = objs[i].copy_from(obj.data, kLargeObjSize);
    assert(succ);
  }

  uint64_t stt, end;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    auto idx = idx_dist(mt);
    auto off = off_dist(mt);

    volatile data_t dst;
    stt = midas::Time::get_cycles_stt();
    {
      data_t dst_;
      objs[idx].copy_to(&dst_, sizeof(dst_), off);
      dst = dst_;
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    if (dur_cycles > kRawMemAccessCycles)
      durs.push_back(dur_cycles);
  }
  printf("Access soft_ptr<LargeObject>:\n");
  print_lats(durs);
}

int main(int argc, char *argv[]) {
  softptr_read_small_cost();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  unique_ptr_read_small_cost();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  unique_ptr_read_large_cost();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  softptr_read_large_cost();

  return 0;
}
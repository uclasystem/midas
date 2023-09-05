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
constexpr static uint64_t kCachePoolSize = 1ull * 1024 * 1024 * 1024;

void unique_ptr_read_small_cost() {
  auto cmanager = midas::CacheManager::global_cache_manager();
  cmanager->create_pool("small");
  auto pool = cmanager->get_pool("small");
  pool->update_limit(kCachePoolSize);
  auto allocator = pool->get_allocator();

  auto obj = std::make_unique<data_t>();

  data_t dst;
  uint64_t stt, end, avg_cycles;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    stt = midas::Time::get_cycles_stt();
    {
      dst = ACCESS_ONCE(*obj.get());
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    avg_cycles += dur_cycles;
  }
  avg_cycles /= kMeasureTimes;
  printf("Access cached unique_ptr<SmallObject> average latency (cycles): %lu\n",
         avg_cycles);
}

void softptr_read_small_cost() {
  auto cmanager = midas::CacheManager::global_cache_manager();
  cmanager->create_pool("small");
  auto pool = cmanager->get_pool("small");
  pool->update_limit(kCachePoolSize);
  auto allocator = pool->get_allocator();

  midas::ObjectPtr obj;
  auto succ = allocator->alloc_to(sizeof(data_t), &obj);
  assert(succ);

  data_t dst;
  uint64_t stt, end, avg_cycles;
  std::vector<uint64_t> durs;
  for (int i = 0; i < kMeasureTimes; i++) {
    stt = midas::Time::get_cycles_stt();
    {
      obj.copy_to(&dst, sizeof(dst));
    }
    end = midas::Time::get_cycles_end();
    auto dur_cycles = end - stt;
    avg_cycles += dur_cycles;
  }
  avg_cycles /= kMeasureTimes;
  printf("Access cached soft_ptr<SmallObject> average latency (cycles): %lu\n",
         avg_cycles);
}

int main(int argc, char *argv[]) {
  unique_ptr_read_small_cost();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  softptr_read_small_cost();

  return 0;
}
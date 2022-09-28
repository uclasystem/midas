#include <atomic>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "log.hpp"

constexpr int kNumThds = 10;
constexpr int kNumObjs = 10240;

int main(int argc, char *argv[]) {
  std::random_device rd;
  std::mt19937 rand(rd());
  std::uniform_int_distribution<> dist(1, 256);

  std::atomic_int nr_errs(0);

  auto *allocator = new cachebank::LogAllocator();
  std::vector<std::thread> threads;
  std::vector<std::optional<cachebank::TransientPtr>> ptrs[kNumThds];
  for (int tid = 0; tid < kNumThds; tid++) {
    threads.push_back(std::thread([&,tid=tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto ret = allocator->alloc(1000);
        if (!ret)
          nr_errs++;
        ptrs[tid].push_back(ret);
      }

      for (auto ptr : ptrs[tid]) {
        bool ret = false;
        if (ptr)
          ret = allocator->free(*ptr);
        if (!ret)
          nr_errs++;
      }
    }));
  }

  for (auto &thd : threads) {
    thd.join();
  }

  if (nr_errs == 0)
    std::cout << "Test passed!" << std::endl;

  return 0;
}
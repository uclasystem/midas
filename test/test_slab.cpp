#include <iostream>
#include <thread>
#include <vector>
#include <random>
#include "slab.hpp"

constexpr int kNumThds = 10;
constexpr int kNumObjs = 1024;

int main(int argc, char *argv[]) {
  cachebank::ResourceManager *rmanager =
      cachebank::ResourceManager::global_manager();
  cachebank::SlabAllocator allocator;

  std::random_device rd;
  std::mt19937 rand(rd());
  std::uniform_int_distribution<> dist(1, 256);

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThds; tid++) {
    thds.push_back(std::thread([&]() {
      std::vector<void *> ptrs;
      for (int i = 0; i < kNumObjs; i++) {
        auto *ptr = allocator.alloc(dist(rand));
        std::cout << "Alloc obj at " << ptr << std::endl;
        ptrs.push_back(ptr);
      }
      for (auto ptr : ptrs) {
        std::cout << "Free obj at " << ptr << std::endl;
        allocator.free(ptr);
      }
    }));
  }

  for (auto &thd : thds)
    thd.join();
  return 0;
}
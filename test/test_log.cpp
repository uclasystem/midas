#include <iostream>
#include <random>
#include <thread>
#include <vector>

#include "resource_manager.hpp"
#include "log.hpp"

constexpr int kNumThds = 10;
constexpr int kNumObjs = 10240;

int main(int argc, char *argv[]) {
  auto *allocator = new cachebank::LogAllocator();

  std::random_device rd;
  std::mt19937 rand(rd());
  std::uniform_int_distribution<> dist(1, 256);

  std::vector<cachebank::TransientPtr> ptrs;
  for (int i = 0; i < kNumObjs; i++) {
    auto ret = allocator->alloc(1000);
    ptrs.push_back(ret);
  }

  std::cout << "Finish allocation!" << std::endl;

  for (auto ptr : ptrs) {
    allocator->free(ptr);
  }

  std::cout << "Finish free!" << std::endl;

  return 0;
}
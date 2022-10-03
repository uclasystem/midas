#include <atomic>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "log.hpp"
#include "transient_ptr.hpp"

constexpr int kNumThds = 10;
constexpr int kNumObjs = 102400;

constexpr int kObjSize = 16;

struct Object {
  char data[kObjSize];

  void random_fill() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < kObjSize; i++) {
      data[i] = dist(mt);
    }
  }

  bool equal(Object &other) {
    return (strncmp(data, other.data, kObjSize) == 0);
  }
};

int main(int argc, char *argv[]) {
  auto *allocator = cachebank::LogAllocator::global_allocator();
  std::vector<std::thread> threads;

  std::atomic_int nr_errs(0);
  std::vector<cachebank::TransientPtr> ptrs[kNumThds];
  std::vector<Object> objs[kNumThds];

  for (int tid = 0; tid < kNumThds; tid++) {
    threads.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto optptr = allocator->alloc(sizeof(Object));
        Object obj;
        obj.random_fill();
        if (!optptr || !(*optptr).copy_from(&obj, sizeof(Object))) {
          nr_errs++;
          continue;
        }
        ptrs[tid].push_back(*optptr);
        objs[tid].push_back(obj);
      }

      for (int i = 0; i < ptrs[tid].size(); i++) {
        bool ret = false;
        auto ptr = ptrs[tid][i];
        Object stored_o;
        if (!ptr.copy_to(&stored_o, sizeof(Object)) ||
            !objs[tid][i].equal(stored_o))
          nr_errs++;
      }

      for (auto ptr : ptrs[tid]) {
        if (!allocator->free(ptr))
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
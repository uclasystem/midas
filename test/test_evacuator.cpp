#include <atomic>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "log.hpp"
#include "object.hpp"
#include "transient_ptr.hpp"
#include "evacuator.hpp"

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
        auto &tptr = *optptr;
        ptrs[tid].push_back(tptr);
        objs[tid].push_back(obj);

        // set rref
        cachebank::SmallObjectHdr hdr;
        auto hdrPtr = tptr.slice(-sizeof(hdr), sizeof(hdr));
        if (!hdrPtr.copy_to(&hdr, sizeof(hdr))) {
          nr_errs++;
          continue;
        }
        hdr.set_rref(
            reinterpret_cast<uint64_t>(&(ptrs[tid].at(ptrs[tid].size() - 1))));
        if (!hdrPtr.copy_from(&hdr, sizeof(hdr))) {
          nr_errs++;
          continue;
        }
      }

      for (int i = 0; i < ptrs[tid].size(); i++) {
        bool ret = false;
        auto ptr = ptrs[tid][i];
        Object stored_o;
        if (!ptr.copy_to(&stored_o, sizeof(Object)) ||
            !objs[tid][i].equal(stored_o))
          nr_errs++;
      }

      // for (auto ptr : ptrs[tid]) {
      //   if (!allocator->free(ptr))
      //     nr_errs++;
      // }
    }));
  }

  for (auto &thd : threads) {
    thd.join();
  }

  cachebank::Evacuator evacuator;
  // evacuator.scan();
  evacuator.evacuate();

  if (nr_errs == 0)
    std::cout << "Test passed!" << std::endl;

  return 0;
}
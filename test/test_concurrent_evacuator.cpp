#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "object.hpp"

constexpr int kNumGCThds = 2;
constexpr int kNumThds = 10;
constexpr int kNumRepeat = 100;
constexpr int kNumObjs = 40960;

constexpr int kObjSize = 111;

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
  std::vector<std::thread> threads;

  std::atomic_int nr_errs(0);
  std::vector<std::shared_ptr<cachebank::ObjectPtr>> ptrs[kNumThds];
  std::vector<Object> objs[kNumThds];

  for (int tid = 0; tid < kNumThds; tid++) {
    threads.push_back(std::thread([&, tid = tid]() {
      auto *allocator = cachebank::LogAllocator::global_allocator();
      for (int i = 0; i < kNumObjs; i++) {
        Object obj;
        obj.random_fill();
        auto objptr = std::make_shared<cachebank::ObjectPtr>();

        if (!allocator->alloc_to(sizeof(Object), objptr.get()) ||
            !objptr->copy_from(&obj, sizeof(Object))) {
          nr_errs++;
          continue;
        }
        ptrs[tid].push_back(objptr);
        objs[tid].push_back(obj);
      }

      for (int i = 0; i < ptrs[tid].size(); i++) {
        bool ret = false;
        auto ptr = ptrs[tid][i];
        Object stored_o;
        if (!ptr->copy_to(&stored_o, sizeof(Object)) ||
            !objs[tid][i].equal(stored_o))
          nr_errs++;
      }
    }));
  }

  for (auto &thd : threads)
    thd.join();
  threads.clear();

  bool stop_evac = false;
  std::thread evac_thd([&]() {
    cachebank::Evacuator evacuator;
    while (!stop_evac) {
      evacuator.scan(kNumGCThds);
      evacuator.evacuate(kNumGCThds);
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  });

  for (int tid = 0; tid < kNumThds; tid++) {
    threads.push_back(std::thread([&, tid = tid]() {
      for (int j = 0; j < kNumRepeat; j++) {
        auto nr_ptrs = ptrs[tid].size();
        for (int i = 0; i < nr_ptrs; i++) {
          auto ptr = ptrs[tid][i];
          Object stored_o;
          if (!ptr || !ptr->copy_to(&stored_o, sizeof(Object)) ||
              !objs[tid][i].equal(stored_o))
            nr_errs++;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }));
  }
  for (auto &thd : threads)
    thd.join();
  threads.clear();
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  stop_evac = true;
  evac_thd.join();

  if (nr_errs == 0)
    std::cout << "Test passed!" << std::endl;
  else
    std::cout << "Test failed, nr_errs: " << nr_errs << std::endl
              << "Note: errors are expected when evacute period is short, and "
                 "objects are evicted."
              << std::endl;
  return 0;
}
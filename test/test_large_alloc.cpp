#include <atomic>
#include <cstring>
#include <iostream>
#include <optional>
#include <random>
#include <thread>
#include <vector>
#include <memory>

#include "log.hpp"
#include "object.hpp"

constexpr static int kNumThds = 10;
constexpr static int kNumObjs = 10240;

constexpr static int kObjMinSize = 8 * 1000;
constexpr static int kObjMaxSize = 9 * 1000;

int random_obj_size() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(kObjMinSize, kObjMaxSize);

  return dist(mt);
}

struct Object {
  int obj_size;
  char *data;

  Object() : Object(random_obj_size()) {}
  Object(int obj_size_) : obj_size(obj_size_) {
    data = new char[obj_size];
  }
  ~Object() {
    delete[] data;
  }

  void random_fill() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    constexpr static int kFillStride = 10;
    for (uint32_t i = 0; i < obj_size / kFillStride; i++) {
      data[i * kFillStride] = dist(mt);
    }
    // memcpy(data, &obj_size, sizeof(int));
  }

  bool equal(Object &other) {
    return (strncmp(data, other.data, obj_size) == 0);
  }
};

std::vector<std::shared_ptr<Object>> objs[kNumThds];
void gen_workload() {
  std::cout << "Generating workload..." << std::endl;
  for (int tid = 0; tid < kNumThds; tid++) {
    for (int i = 0; i < kNumObjs; i++) {
      auto obj = std::make_shared<Object>();
      obj->random_fill();
      objs[tid].push_back(obj);
    }
  }
  std::cout << "Finish generating workload!" << std::endl;
}

int main(int argc, char *argv[]) {
  auto *allocator = cachebank::LogAllocator::global_allocator();

  gen_workload();

  std::atomic_int nr_errs(0);
  std::atomic_int nr_eqs(0);
  std::atomic_int nr_neqs(0);
  std::vector<std::shared_ptr<cachebank::ObjectPtr>> ptrs[kNumThds];

  std::vector<std::thread> threads;
  for (int tid = 0; tid < kNumThds; tid++) {
    threads.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        const auto &obj = objs[tid][i];
        const auto size = obj->obj_size;

        bool ret = false;

        auto objptr = std::make_shared<cachebank::ObjectPtr>();
        ptrs[tid].push_back(objptr);
        if (!allocator->alloc_to(size, objptr.get()) ||
            !(ret = objptr->copy_from(obj->data, size))) {
          nr_errs++;
        }
      }

      for (int i = 0; i < ptrs[tid].size(); i++) {
        bool ret = false;
        const auto &obj = objs[tid][i];
        const auto size = obj->obj_size;

        auto ptr = ptrs[tid][i];
        Object stored_o(size);
        if (!ptr->copy_to(stored_o.data, size)) {
          nr_errs++;
          continue;
        }
        if (!objs[tid][i]->equal(stored_o)) {
          nr_neqs++;
        } else {
          nr_eqs++;
        }
      }

      for (auto ptr : ptrs[tid]) {
        if (!allocator->free(*ptr))
          // nr_errs++;
          continue;
      }
    }));
  }

  for (auto &thd : threads) {
    thd.join();
  }

  std::cout << "Equal/Not-equal/Total: " << nr_eqs << "/" << nr_neqs << "/"
            << kNumObjs * kNumThds << std::endl;
  if (nr_errs == 0)
    std::cout << "Test passed!" << std::endl;
  else
    std::cout << "Test failed: " << nr_errs << "/" << kNumObjs * kNumThds
              << " failed cases." << std::endl;

  return 0;
}
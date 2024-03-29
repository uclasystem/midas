#include <atomic>
#include <functional>
#include <iostream>
#include <thread>
#include <vector>

#include "soft_mem_pool.hpp"
#include "soft_unique_ptr.hpp"

constexpr static int kNumThds = 16;
constexpr static int kObjSize = 64;
constexpr static int kNumObjs = 10240;

struct Object {
  int data[kObjSize / sizeof(int)];
};

int recon_int(int a, float d) { return a + int(d); }

struct Object recon_obj(int a, uint64_t b) {
  struct Object obj;
  int c = static_cast<int>(b & 0xFFFFFFFF);
  for (int i = 0; i < kObjSize / sizeof(int); i++) {
    obj.data[i] = a + c + i;
  }
  return obj;
}

void test_int_read() {
  auto smanager = midas::SoftMemPoolManager::global_soft_mem_pool_manager();
  auto pool = smanager->get_pool<int, int, float>("int");

  std::vector<std::thread> thds;
  std::atomic_int32_t nr_failed(0);

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&pool, &nr_failed]() {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist(0, 1 << 30);
      using SoftUniquePtr = midas::SoftUniquePtr<int, int, float>;
      std::vector<SoftUniquePtr> ptrs;

      for (int j = 0; j < kNumObjs; j++) {
        auto ptr = pool->new_unique();
        ptrs.push_back(std::move(ptr));
      }
      for (int j = 0; j < kNumObjs; j++) {
        int a = dist(mt);
        float b = dist(mt);
        int recon_value = recon_int(a, b);
        int value = ptrs[j].read(a, b);
        if (value != recon_value)
          nr_failed++;
      }
      for (int j = 0; j < kNumObjs; j++) {
        ptrs[j].reset();
      }
    });
  }
  for (auto &thd : thds)
    thd.join();

  if (nr_failed == 0) {
    std::cout << "Test int read passed." << std::endl;
  } else {
    std::cout << "Test int read failed " << nr_failed << std::endl;
  }
}


void test_int_write() {
  auto smanager = midas::SoftMemPoolManager::global_soft_mem_pool_manager();
  auto pool = smanager->get_pool<int, int, float>("int");

  std::vector<std::thread> thds;
  std::atomic_int32_t nr_failed(0);

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&pool, &nr_failed]() {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist(0, 1 << 30);
      using SoftUniquePtr = midas::SoftUniquePtr<int, int, float>;
      std::vector<SoftUniquePtr> ptrs;
      std::vector<int> written_vals;

      for (int j = 0; j < kNumObjs; j++) {
        auto ptr = pool->new_unique();
        int value = dist(mt);
        ptr.write(value);
        ptrs.push_back(std::move(ptr));
        written_vals.push_back(value);
      }
      for (int j = 0; j < kNumObjs; j++) {
        int a = dist(mt);
        float b = dist(mt);
        int written_value = written_vals[j];
        int value = ptrs[j].read(a, b);
        if (value != written_value) {
          int recon_value = recon_int(a, b);
          std::cout << "value: " << value << " written_value: " << written_value << " recon_value: " << recon_value
                    << std::endl;
          nr_failed++;
        }
      }
      for (int j = 0; j < kNumObjs; j++) {
        ptrs[j].reset();
      }
    });
  }
  for (auto &thd : thds)
    thd.join();

  if (nr_failed == 0) {
    std::cout << "Test int write passed." << std::endl;
  } else {
    std::cout << "Test int write failed " << nr_failed << std::endl;
  }
}

void test_obj_read() {
  auto smanager = midas::SoftMemPoolManager::global_soft_mem_pool_manager();
  auto pool = smanager->get_pool<struct Object, int, uint64_t>("object");

  std::vector<std::thread> thds;
  std::atomic_int32_t nr_failed(0);

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&pool, &nr_failed]() {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist(0, 1 << 30);
      using SoftUniquePtr = midas::SoftUniquePtr<struct Object, int, uint64_t>;
      std::vector<SoftUniquePtr> ptrs;

      for (int j = 0; j < kNumObjs; j++) {
        auto ptr = pool->new_unique();
        ptrs.push_back(std::move(ptr));
      }
      for (int j = 0; j < kNumObjs; j++) {
        int a = dist(mt);
        uint64_t b = dist(mt);
        struct Object recon_value = recon_obj(a, b);
        struct Object value = ptrs[j].read(a, b);
        for (int k = 0; k < kObjSize / sizeof(int); k++) {
          if (value.data[k] != recon_value.data[k]) {
            nr_failed++;
            break;
          }
        }
      }
      for (int j = 0; j < kNumObjs; j++) {
        ptrs[j].reset();
      }
    });
  }
  for (auto &thd : thds)
    thd.join();

  if (nr_failed == 0) {
    std::cout << "Test obj read passed." << std::endl;
  } else {
    std::cout << "Test obj read failed " << nr_failed << std::endl;
  }
}

void test_obj_write() {
  auto smanager = midas::SoftMemPoolManager::global_soft_mem_pool_manager();
  auto pool = smanager->get_pool<struct Object, int, uint64_t>("object");

  std::vector<std::thread> thds;
  std::atomic_int32_t nr_failed(0);

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&pool, &nr_failed]() {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<int> dist(0, 1 << 30);
      using SoftUniquePtr = midas::SoftUniquePtr<struct Object, int, uint64_t>;
      std::vector<SoftUniquePtr> ptrs;
      std::vector<struct Object> written_vals;

      for (int j = 0; j < kNumObjs; j++) {
        auto ptr = pool->new_unique();
        struct Object value;
        for (int k = 0; k < kObjSize / sizeof(int); k++) {
          value.data[k] = dist(mt);
        }
        ptr.write(value);
        ptrs.push_back(std::move(ptr));
        written_vals.push_back(value);
      }
      for (int j = 0; j < kNumObjs; j++) {
        int a = dist(mt);
        uint64_t b = dist(mt);
        struct Object written_value = written_vals[j];
        struct Object value = ptrs[j].read(a, b);
        for (int k = 0; k < kObjSize / sizeof(int); k++) {
          if (value.data[k] != written_value.data[k]) {
            struct Object recon_value = recon_obj(a, b);
            std::cout << "value: " << value.data[k] << " written_value: " << written_value.data[k]
                      << " recon_value: " << recon_value.data[k] << std::endl;
            nr_failed++;
            break;
          }
        }
      }
      for (int j = 0; j < kNumObjs; j++) {
        ptrs[j].reset();
      }
    });
  }
  for (auto &thd : thds)
    thd.join();

  if (nr_failed == 0) {
    std::cout << "Test obj write passed." << std::endl;
  } else {
    std::cout << "Test obj write failed " << nr_failed << std::endl;
  }
}

int main(int argc, char *argv[]) {

  auto smanager = midas::SoftMemPoolManager::global_soft_mem_pool_manager();

  smanager->create_pool<int, int, float>("int", recon_int);
  test_int_read();
  test_int_write();

  smanager->create_pool<Object, int, uint64_t>("object", recon_obj);
  test_obj_read();
  test_obj_write();

  return 0;
}
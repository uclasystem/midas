#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "resource_manager.hpp"
#include "slab.hpp"
#include "sync_hashmap.hpp"

constexpr int kNBuckets = (1 << 20);
constexpr int kNumThds = 10;
constexpr int kNumObjs = 1024;
constexpr int kKLen = 20;
constexpr int kVLen = 2;

std::random_device rd;
std::mt19937 mt(rd());
std::uniform_int_distribution<int> dist('A', 'z');

std::string random_str(uint32_t len) {
  std::string str = "";
  for (uint32_t i = 0; i < len; i++) {
    str += dist(mt);
  }
  return str;
}

int main(int argc, char *argv[]) {
  auto *rmanager = cachebank::ResourceManager::global_manager();
  // auto *allocator = cachebank::ResourceManager::global_allocator();
  // auto *hashmap = new cachebank::SyncHashMap<kNBuckets, std::string,
  // std::string>();
  using K = std::string;
  using V = std::string;
  // using K = int;
  // using V = int;
  auto *hashmap = new cachebank::SyncHashMap<kNBuckets, K, V>();

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      std::unordered_map<K, V> kvs;
      for (int i = 0; i < kNumObjs; i++) {
        K k = random_str(kKLen);
        std::string v = random_str(kVLen);
        // K k = i + tid * kNumObjs;
        // V v = 1;
        kvs[k] = v;
        bool ret = hashmap->set(k, v);
        if (!ret)
          std::cout << "Hash map set (" << k << "," << v << ") ret: " << ret
                    << std::endl;
      }
      for (auto &pair : kvs) {
        const K &k = pair.first;
        V &v = pair.second;
        V *v2 = hashmap->get(k);
        // if (!v2 || *v2 != v)
        //   std::cout << "Hash map get failed, key = " << k << "values: " <<
        //   *v2
        //             << " != " << v << std::endl;
      }
      for (auto &pair : kvs) {
        const K &k = pair.first;
        if (!hashmap->remove(k))
          std::cout << "Hash map remove failed, key = " << k << std::endl;
      }
    }));
  }

  for (auto &thd : thds)
    thd.join();
  return 0;
}
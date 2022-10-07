#include <atomic>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "resource_manager.hpp"
#include "slab.hpp"
#include "sync_hashmap.hpp"

constexpr int kNBuckets = (1 << 20);
constexpr int kNumInsertThds = 20;
constexpr int kNumRemoveThds = 20;
constexpr int kNumObjs = 409600;
constexpr int kKLen = 18;
constexpr int kVLen = 31;

template <int Len> struct Object;

// using K = int;
// using V = int;
using K = Object<kKLen>;
using V = Object<kVLen>;

/** YIFAN: string is not supported! Its reference will be lost after data copy
 */
// using K = std::string;
// using V = std::string;

template <int Len> struct Object {
  char data[Len];
  bool operator==(const Object &other) const {
    return strncmp(data, other.data, Len) == 0;
  }
  bool operator!=(const Object &other) const {
    return strncmp(data, other.data, Len) != 0;
  }
  bool operator<(const Object &other) const {
    return strncmp(data, other.data, Len) < 0;
  }
  void random_fill() {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < Len - 1; i++)
      data[i] = dist(mt);
    data[Len - 1] = 0;
  }
};

template <class K> K get_K() { return K(); }
template <class V> V get_V() { return V(); }

template <> std::string get_K() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist('A', 'z');

  std::string str = "";
  for (uint32_t i = 0; i < kKLen - 1; i++)
    str += dist(mt);
  str += '\0';
  return str;
}

template <> std::string get_V() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist('A', 'z');

  std::string str = "";
  for (uint32_t i = 0; i < kVLen - 1; i++)
    str += dist(mt);
  str += '\0';
  return str;
}

template <> int get_K() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(0, 1 << 30);
  return dist(mt);
}

template <> int get_V() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(0, 1 << 30);
  return dist(mt);
}

template <> Object<kKLen> get_K() {
  Object<kKLen> k;
  k.random_fill();
  return k;
}

template <> Object<kVLen> get_V() {
  Object<kVLen> v;
  v.random_fill();
  return v;
}
namespace std {
template <> struct hash<Object<kKLen>> {
  size_t operator()(const Object<kKLen> &k) const {
    return std::hash<std::string_view>()(
        std::string_view(k.data, strlen(k.data)));
  }
};
} // namespace std

int main(int argc, char *argv[]) {
  auto *rmanager = cachebank::ResourceManager::global_manager();
  std::vector<std::thread> thds;

  auto *hashmap = new cachebank::SyncHashMap<kNBuckets, K, V>();
  std::mutex std_map_lock;
  std::unordered_map<K, V> std_map;

  std::atomic_int32_t nr_succ = 0;
  std::atomic_int32_t nr_err = 0;

  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto k = get_K<K>();
        auto v = get_V<V>();
        std_map_lock.lock();
        std_map[k] = v;
        std_map_lock.unlock();
        bool ret = hashmap->set(k, v);
        if (!ret)
          nr_err++;
        else
          nr_succ++;
      }
    }));
  }
  for (auto &thd : thds)
    thd.join();
  thds.clear();

  if (nr_err == 0)
    std::cout << "Set test passed!" << std::endl;
  else
    std::cout << "Set test failed! " << nr_succ << " passed, " << nr_err
              << " failed." << std::endl;

  nr_succ = nr_err = 0;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&]() {
      for (auto &pair : std_map) {
        const K &k = pair.first;
        V &v = pair.second;
        auto v2 = hashmap->get(k);

        if (!v2 || v != *v2) {
          nr_err++;
        } else
          nr_succ++;
      }
    }));
  }
  for (auto &thd : thds)
    thd.join();
  thds.clear();

  if (nr_err == 0)
    std::cout << "Get test passed!" << std::endl;
  else
    std::cout << "Get test failed! " << nr_succ << " passed, " << nr_err
              << " failed." << std::endl
              << "NOTE: a small amount of failures are expected if only "
                 "std_map is protected by lock, as keys can conflict in our "
                 "sync_hash_map and the result of races are uncertain."
              << std::endl;
        ;

  std::vector<std::pair<K, V>> pairs;
  for (auto pair : std_map) {
    pairs.push_back(pair);
  }

  nr_succ = nr_err = 0;
  for (int tid = 0; tid < kNumRemoveThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      int chunk = (pairs.size() + kNumRemoveThds - 1) / kNumRemoveThds;
      int stt = chunk * tid;
      int end = std::min<int>(stt + chunk, pairs.size());

      for (int i = stt; i < end; i++) {
        auto pair = pairs[i];
        const K &k = pair.first;
        bool ret = hashmap->remove(k);
        if (!ret)
          nr_err++;
        else
          nr_succ++;
      }
    }));
  }
  for (auto &thd : thds)
    thd.join();

  if (nr_err == 0)
    std::cout << "Remove test passed!" << std::endl;
  else
    std::cout << "Remove test failed! " << nr_succ << " passed, " << nr_err
              << " failed." << std::endl;
  return 0;
}
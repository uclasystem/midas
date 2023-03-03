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

constexpr int kNBuckets = (1 << 16);
constexpr int kNumInsertThds = 40;
constexpr int kNumObjs = 102400;
constexpr int kKLen = 61;
constexpr int kVLen = 10;

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

struct Op {
  enum OpCode { Set, Get, Remove } opcode;
  K key;
  V val;
};
std::vector<K> ks[kNumInsertThds];
std::vector<V> vs[kNumInsertThds];
std::vector<Op> ops[kNumInsertThds];

void gen_workload() {
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    for (int o = 0; o < kNumObjs; o++) {
      K k = get_K<K>();
      V v = get_V<V>();
      ks[tid].push_back(k);
      vs[tid].push_back(v);
      Op op{.opcode = Op::Set, .key = k, .val = v};
      ops[tid].push_back(op);
    }
  }
  for (auto &thd : thds)
    thd.join();
  std::cout << "Finish generate workload." << std::endl;
}

int main(int argc, char *argv[]) {
  auto *rmanager = midas::ResourceManager::global_manager();
  std::vector<std::thread> thds;

  auto *hashmap = new midas::SyncHashMap<kNBuckets, K, V>();
  std::mutex std_map_lock;
  std::unordered_map<K, V> std_map;

  std::atomic_int32_t nr_succ = 0;
  std::atomic_int32_t nr_err = 0;

  gen_workload();

  hashmap->clear();
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto &k = ks[tid][i];
        auto &v = vs[tid][i];
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

  if (hashmap->clear()) {
    std::cout << "Clear test passed!" << std::endl;
  } else {
    std::cout << "Clear test failed!" << std::endl;
  }

  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto &k = ks[tid][i];
        auto &v = vs[tid][i];
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
    std::cout << "2nd round Set test passed!" << std::endl;
  else
    std::cout << "2nd round Set test failed! " << nr_succ << " passed, " << nr_err
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
    std::cout << "2nd round Get test passed!" << std::endl;
  else
    std::cout << "2nd round Get test failed! " << nr_succ << " passed, " << nr_err
              << " failed." << std::endl
              << "NOTE: a small amount of failures are expected if only "
                 "std_map is protected by lock, as keys can conflict in our "
                 "sync_hash_map and the result of races are uncertain."
              << std::endl;

  return 0;
}
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
#include "sync_kv.hpp"

#define TEST_OBJECT 1
#define TEST_LARGE 1

constexpr static size_t kCacheSize = 1024ull * 1024 * 200;
constexpr static int kNBuckets = (1 << 20);
constexpr static int kNumInsertThds = 10;
constexpr static int kNumRemoveThds = 10;

#if TEST_LARGE
constexpr static int kNumObjs = 10240;
constexpr static int kKLen = 32;
constexpr static int kVLen = 8192;
#else // !TEST_LARGE
constexpr static int kNumObjs = 102400;
constexpr static int kKLen = 61;
constexpr static int kVLen = 10;
#endif // TEST_LARGE

template <int Len> struct Object;

#if TEST_OBJECT
using K = Object<kKLen>;
using V = Object<kVLen>;
#else // !TEST_OBJECT
using K = int;
using V = int;
#endif // TEST_OBJECT

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
  str.reserve(kKLen);
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
  str.reserve(kVLen);
#if TEST_LARGE == 1
  constexpr static int kFillStride = 2000;
  for (uint32_t i = 0; i < kVLen - 1; i++)
    if (i % kFillStride == 0)
      str += dist(mt);
    else
      str += static_cast<char>(i % ('z' - 'A')) + 'A';
#else // !TEST_LARGE
  for (uint32_t i = 0; i < kVLen - 1; i++)
    str += dist(mt);
#endif
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
  rmanager->UpdateLimit(kCacheSize);

  auto *kvstore = new midas::SyncKV<kNBuckets>();
  std::mutex std_map_lock;
  std::unordered_map<K, V> std_map;

  std::atomic_int32_t nr_succ = 0;
  std::atomic_int32_t nr_err = 0;

  gen_workload();

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto &k = ks[tid][i];
        auto &v = vs[tid][i];
        std_map_lock.lock();
        std_map[k] = v;
        std_map_lock.unlock();
        bool ret = kvstore->set(&k, sizeof(k), &v, sizeof(v));
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

  std::atomic_int32_t nr_equal = 0;
  std::atomic_int32_t nr_nequal = 0;
  nr_succ = nr_err = 0;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&]() {
      for (auto &pair : std_map) {
        const K &k = pair.first;
        V &v = pair.second;
        size_t stored_vn = 0;
        V *v2 = reinterpret_cast<V *>(kvstore->get(&k, sizeof(k), &stored_vn));
        if (!v2) {
          nr_err++;
        } else {
          nr_succ++;
          assert(stored_vn == sizeof(V));
          if (v == *v2) {
            nr_equal++;
          } else {
            nr_nequal++;
          }
        }
        if (v2)
          free(v2);
      }
    }));
  }
  for (auto &thd : thds)
    thd.join();
  thds.clear();

  if (nr_nequal == 0)
    std::cout << "Get test passed! " << nr_succ << " passed, " << nr_err
              << " failed. " << nr_equal << "/" << nr_equal + nr_nequal
              << " pairs are equal" << std::endl;
  else
    std::cout << "Get test failed! " << nr_succ << " passed, " << nr_err
              << " failed. " << nr_equal << "/" << nr_equal + nr_nequal
              << " pairs are equal" << std::endl
              << "NOTE: a small amount of failures are expected if only "
                 "std_map is protected by lock, as keys can conflict in our "
                 "sync_hash_map and the result of races are uncertain."
              << std::endl;

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
        bool ret = kvstore->remove(&k, sizeof(k));
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
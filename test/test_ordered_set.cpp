#include <atomic>
#include <iostream>
#include <iterator>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "resource_manager.hpp"
#include "sync_kv.hpp"

#define TEST_OBJECT 0
#define TEST_LARGE 0

constexpr static size_t kCacheSize = 1024ull * 1024 * 2000;
constexpr static int kNBuckets = (1 << 20);
constexpr static int kOSetSize = 20;
constexpr static int kNumInsertThds = 10;
constexpr static int kNumRemoveThds = 1;

#if TEST_OBJECT
#if TEST_LARGE
constexpr static int kNumObjs = 1024;
constexpr static int kKLen = 32;
constexpr static int kVLen = 8192;
#else // !TEST_LARGE
constexpr static int kNumObjs = 1024;
constexpr static int kKLen = 61;
constexpr static int kVLen = 10;
#endif // TEST_LARGE
#else // !TEST_OBJECT
constexpr static int kNumObjs = 10240;
#endif

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

#if TEST_OBJECT
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
#endif // TEST_OBJECT

struct Op {
  enum OpCode { Set, Get, Remove } opcode;
  K key;
  V vals[kOSetSize];
  double scores[kOSetSize];
};
std::vector<Op> ops[kNumInsertThds];

void gen_workload() {
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    std::random_device rd;
    std::mt19937 mt(rd());
    std::uniform_real_distribution<double> dist(0.0, 100.0);
    for (int o = 0; o < kNumObjs; o++) {
      K k = get_K<K>();
      Op op{.opcode = Op::Set, .key = k};
      for (int j = 0; j < kOSetSize; j++) {
        op.vals[j] = get_V<V>();
        op.scores[j] = (double)op.vals[j];
        // op.scores[j] = dist(mt);
      }
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

  std::atomic_int32_t nr_succ{0};
  std::atomic_int32_t nr_err{0};

  gen_workload();

  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        auto &k = ops[tid][i].key;
        auto &vs = ops[tid][i].vals;
        for (int j = 0; j < kOSetSize; j++) {
          bool ret = kvstore->zadd(
              &k, sizeof(k), &vs[j], sizeof(vs[j]), ops[tid][i].scores[j],
              midas::SyncKV<kNBuckets>::UpdateType::NOT_EXIST);
          if (!ret)
            nr_err++;
          else
            nr_succ++;
        }
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

  std::atomic_int32_t nr_equal{0};
  std::atomic_int32_t nr_nequal{0};
  nr_succ = nr_err = 0;
  for (int tid = 0; tid < kNumInsertThds; tid++) {
    thds.push_back(std::thread([&, tid=tid]() {
      for (int i = 0; i < kNumObjs; i++) {
        const auto &op = ops[tid][i];
        const K &k = op.key;
        size_t stored_vn = 0;
        std::vector<midas::kv_types::Value> raw_vs;
        std::vector<V> vs;
        if (!kvstore->zrange(&k, sizeof(k), 0, kOSetSize - 1,
                             std::back_inserter(raw_vs))) {
          nr_err++;
        } else {
          nr_succ++;
        }
        for (auto [vp, size] : raw_vs) {
          // std::cout << *(V *)(vp) << " " << size << std::endl;
          vs.emplace_back(*(V *)vp);
          free(vp);
        }
        bool sorted = true;
        for (int j = 0; j < vs.size() - 1; j++) {
          if (vs[j] > vs[j + 1]) {
            sorted = false;
            break;
          }
        }
        if (sorted)
          nr_equal++;
        else
          nr_nequal++;
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

  nr_succ = nr_err = 0;
  for (int tid = 0; tid < kNumRemoveThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (auto &op : ops[tid]) {
        const K &k = op.key;
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
#include <atomic>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "sync_list.hpp"

#define TEST_OBJECT 1
#define TEST_LARGE 0

constexpr static int kNumThds = 10;

#if TEST_LARGE
constexpr static int kNumObjs = 10240;
constexpr static int kVLen = 8192;
#else  // !TEST_LARGE
constexpr static int kNumObjs = 102400;
constexpr static int kVLen = 128;
#endif // TEST_LARGE

template <int Len> struct Object;

#if TEST_OBJECT
using V = Object<kVLen>;
#else  // !TEST_OBJECT
using V = int;
#endif // TEST_OBJECT

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

#if TEST_LARGE
    const int STRIDE = 100;
    for (uint32_t i = 0; i < (Len - 1) / STRIDE; i++)
      data[i * STRIDE] = dist(mt);
#else
    for (uint32_t i = 0; i < Len - 1; i++)
      data[i] = dist(mt);
#endif
    data[Len - 1] = 0;
  }
};

template <class V> V get_V() { return V(); }

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

template <> int get_V() {
  static std::random_device rd;
  static std::mt19937 mt(rd());
  static std::uniform_int_distribution<int> dist(0, 1 << 30);
  return dist(mt);
}

template <> Object<kVLen> get_V() {
  Object<kVLen> v;
  v.random_fill();
  return v;
}

namespace std {
template <> struct hash<Object<kVLen>> {
  size_t operator()(const Object<kVLen> &k) const {
    return std::hash<std::string_view>()(
        std::string_view(k.data, strlen(k.data)));
  }
};
} // namespace std

std::vector<V> vs[kNumThds];
std::unordered_map<V, bool> vmap;

void gen_workload() {
  for (int tid = 0; tid < kNumThds; tid++) {
    for (int o = 0; o < kNumObjs; o++) {
      V v = get_V<V>();
      vs[tid].emplace_back(v);
      vmap[v] = true;
    }
  }
  std::cout << "Finish generate workload." << std::endl;
}

int main(int argc, char *argv[]) {
  std::vector<std::thread> thds;
  auto *list = new midas::SyncList<V>();

  std::atomic_int32_t nr_succ = 0;
  std::atomic_int32_t nr_fail = 0;
  std::atomic_int32_t nr_err = 0;

  gen_workload();

  for (int tid = 0; tid < kNumThds; tid++) {
    thds.push_back(std::thread([&, tid = tid] {
      for (auto &v : vs[tid]) {
        if (list->push(v))
          nr_succ++;
        else
          nr_fail++;
      }
    }));
  }

  for (int oid = 0; oid < kNumObjs; oid++) {
    while (list->empty())
      ;
    auto v = list->pop();
    if (!v) {
      nr_fail++;
      continue;
    }
    if (vmap[*v])
      nr_succ++;
    else
      nr_err++;
  }

  for (auto &thd : thds) {
    thd.join();
  }
  thds.clear();

  for (int tid = 0; tid < kNumThds - 1; tid++) {
    thds.push_back(std::thread([&, tid = tid] {
      for (int oid = 0; oid < kNumObjs; oid++) {
        while (list->empty())
          ;
        auto v = list->pop();
        if (!v) {
          nr_fail++;
          continue;
        }
        if (vmap[*v])
          nr_succ++;
        else
          nr_err++;
      }
    }));
  }

  for (auto &thd : thds) {
    thd.join();
  }

  if (nr_err == 0 && nr_fail == 0)
    std::cout << "Test passed! ";
  else
    std::cout << "Test failed! ";
  std::cout << nr_succ << " passed, " << nr_fail << " failed, " << nr_err
            << " wrong results." << std::endl;

  return 0;
}
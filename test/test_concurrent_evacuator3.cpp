#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "evacuator.hpp"
#include "log.hpp"
#include "resource_manager.hpp"
#include "slab.hpp"
#include "sync_hashmap.hpp"

constexpr int kNBuckets = (1 << 20);
constexpr int kNumMutatorThds = 20;
constexpr int kNumGCThds = 3;
constexpr int kNumOps = 409600;
constexpr int kKLen = 18;
constexpr int kVLen = 31;

constexpr float kSetRatio = 70;
constexpr float kGetRatio = 20;
constexpr float kRmvRatio = 10;

template <int Len> struct Object;

using K = Object<kKLen>;
using V = Object<kVLen>;

/** Define Object */
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

Object<kKLen> get_K() {
  Object<kKLen> k;
  k.random_fill();
  return k;
}
Object<kVLen> get_V() {
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
/** Define Object [End] */

/** Generate requests */
struct Op {
  enum OpCode { Set, Get, Remove } opcode;
  K key;
  V val;
};
std::vector<K> ks[kNumMutatorThds];
std::vector<V> vs[kNumMutatorThds];
std::vector<Op> ops[kNumMutatorThds];

void gen_workload() {
  for (int tid = 0; tid < kNumMutatorThds; tid++) {
    for (int o = 0; o < kNumOps; o++) {
      K k = get_K();
      V v = get_V();
      ks[tid].push_back(k);
      vs[tid].push_back(v);
      Op op{.opcode = Op::Set, .key = k, .val = v};
      ops[tid].push_back(op);
      op.opcode = Op::Get;
      ops[tid].push_back(op);
      // op.opcode = Op::Remove;
      // ops[tid].push_back(op);
    }

    for (int o = 0; o < kNumOps; o++) {
      Op op{.opcode = Op::Get, .key = ks[tid][o], .val = vs[tid][o]};
      ops[tid].push_back(op);
    }

    // for (int o = 0; o < kNumOps; o++) {
    //   Op op { .opcode = Op::Remove, .key = ks[tid][o], .val = vs[tid][o] };
    //   ops[tid].push_back(op);
    // }
  }
  std::cout << "Finish generate workload." << std::endl;
}

int main(int argc, char *argv[]) {
  auto *rmanager = cachebank::ResourceManager::global_manager();
  std::vector<std::thread> thds;

  gen_workload();
  bool stop = false;
  std::thread evac_thd([&]() {
    cachebank::Evacuator evacuator;
    while (!stop) {
      evacuator.scan(kNumGCThds);
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      evacuator.evacuate(kNumGCThds);
    }
  });

  auto *hashmap = new cachebank::SyncHashMap<kNBuckets, K, V>();
  std::unordered_map<K, V> std_maps[kNumMutatorThds];

  std::atomic_int32_t nr_succ = 0;
  std::atomic_int32_t nr_err = 0;

  for (int tid = 0; tid < kNumMutatorThds; tid++) {
    thds.push_back(std::thread([&, tid = tid]() {
      for (auto &op : ops[tid]) {
        auto k = op.key;
        auto v = op.val;
        bool ret = true;
        if (op.opcode == Op::Set)
          ret = hashmap->set(k, v);
        else if (op.opcode == Op::Get) {
          auto v = hashmap->get(k);
        } else if (op.opcode == Op::Remove) {
          hashmap->remove(k);
        } else {
          std::cerr << "should not reach here!" << std::endl;
        }

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

  std::this_thread::sleep_for(std::chrono::milliseconds(4000));
  stop = true;
  evac_thd.join();

  if (nr_err == 0)
    std::cout << "Test passed!" << std::endl;
  else
    std::cout << "Tet test failed! " << nr_succ << " passed, " << nr_err
              << " failed." << std::endl;

  return 0;
}
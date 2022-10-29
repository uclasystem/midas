#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
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
#include "sync_hashmap.hpp"
#include "timer.hpp"
#include "utils.hpp"
#include "zipf.hpp"

constexpr static double kZipfSkew = 0.9;

constexpr static int kNBuckets = (1 << 28);
constexpr static int kNumMutatorThds = 40;
constexpr static int kNumGCThds = 8;
constexpr static int kNumTotalKVPairs = 64 * 1024 * 1024;
constexpr static int kNumOps = 8 * 1024 * 1024;
constexpr static int kKLen = 18;
constexpr static int kVLen = 61;

constexpr static int kNumKVPairs = kNumTotalKVPairs / kNumMutatorThds;

static std::unique_ptr<std::mt19937> mts[kNumMutatorThds];

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
  void random_fill(int tid) {
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < Len - 1; i++)
      data[i] = dist(*mts[tid]);
    data[Len - 1] = 0;
  }
};

Object<kKLen> get_K(int tid) {
  Object<kKLen> k;
  k.random_fill(tid);
  return k;
}
Object<kVLen> get_V(int tid) {
  Object<kVLen> v;
  v.random_fill(tid);
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

class CachebankTest {
private:
  cachebank::SyncHashMap<kNBuckets, K, V> *hashmap;
  std::unordered_map<K, V> std_maps[kNumMutatorThds];

  std::vector<K> ks[kNumMutatorThds];
  std::vector<V> vs[kNumMutatorThds];
  std::vector<Op> ops[kNumMutatorThds];
  std::vector<int> zipf_idxes[kNumMutatorThds];

  struct alignas(64) Stats {
    int64_t nr_succ;
    int64_t nr_err;
    int64_t nr_hit;
    int64_t nr_miss;

    Stats() : nr_succ(0), nr_err(0), nr_hit(0), nr_miss(0) {}
    void reset() { nr_succ = nr_err = nr_hit = nr_miss = 0; }
    Stats &accum(const Stats &rhs) {
      nr_succ += rhs.nr_succ;
      nr_err += rhs.nr_err;
      nr_hit += rhs.nr_hit;
      nr_miss += rhs.nr_miss;
      return *this;
    }
  };
  Stats stats;

  bool stop;
  std::shared_ptr<std::thread> evac_thd;

  void reset() {
    for (int i = 0; i < kNumMutatorThds; i++) {
      std::random_device rd;
      mts[i].reset(new std::mt19937(rd()));
    }

    hashmap = new cachebank::SyncHashMap<kNBuckets, K, V>();
    stop = false;

    stats.reset();
  }

  void prepare() {
    stats.reset();

    Stats perthd_stats[kNumMutatorThds];
    std::vector<std::thread> thds;
    for (int tid = 0; tid < kNumMutatorThds; tid++) {
      thds.push_back(std::thread([&, tid = tid]() {
        for (int o = 0; o < kNumKVPairs; o++) {
          const K &k = get_K(tid);
          const V &v = get_V(tid);
          ks[tid].emplace_back(k);
          vs[tid].emplace_back(v);
          if (hashmap->set(k, v)) {
            perthd_stats[tid].nr_succ++;
            auto _ = hashmap->get(k);
            // hashmap->get(k);
            // hashmap->get(k);
          }
          else
            perthd_stats[tid].nr_err++;
        }
      }));
    }
    for (auto &thd : thds)
      thd.join();
    std::cout << "Finish preparation." << std::endl;
    for (int i = 0; i < kNumMutatorThds; i++) {
      stats.accum(perthd_stats[i]);
    }
    if (stats.nr_err > 0)
      std::cout << "Set succ " << stats.nr_succ << ", fail " << stats.nr_err
                << std::endl;
    // std::this_thread::sleep_for(std::chrono::seconds(5));
  }

  void gen_load() {
    std::vector<std::thread> thds;
    for (int tid = 0; tid < kNumMutatorThds; tid++) {
      thds.push_back(std::thread([&, tid = tid]() {
        cachebank::zipf_table_distribution<> dist(kNumKVPairs, kZipfSkew);
        // std::uniform_int_distribution<> dist(0, kNumKVPairs);
        zipf_idxes[tid].clear();
        for (int o = 0; o < kNumOps; o++) {
          auto idx = dist(*mts[tid]);
          zipf_idxes[tid].push_back(idx);
        }
      }));
    }
    for (auto &thd : thds)
      thd.join();
    std::cout << "Finish load generation." << std::endl;
  }

  void launch_evacuator() {
    evac_thd = std::make_shared<std::thread>([&]() {
      auto evacuator = cachebank::Evacuator::global_evacuator();
      while (!stop) {
        evacuator->scan(kNumGCThds);
        // evacuator->evacuate(kNumGCThds);
        // evacuator->conc_gc(kNumGCThds);
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    });
  }

public:
  void init() {
    reset();
    launch_evacuator();
    prepare();
    // gen_load();
  }

  void finalize() {
    stop = true;
    if (evac_thd)
      evac_thd->join();
    evac_thd.reset();
  }

  int run() {
    stats.reset();
    gen_load();

    auto stt = std::chrono::steady_clock::now();

    Stats perthd_stats[kNumMutatorThds];
    std::vector<std::thread> thds;
    for (int tid = 0; tid < kNumMutatorThds; tid++) {
      thds.push_back(std::thread([&, tid = tid]() {
        for (auto idx : zipf_idxes[tid]) {
          bool ret = false;

          auto &k = ks[tid][idx];
          auto &v = vs[tid][idx];
          auto optv = hashmap->get(k);
          if (optv) {
            ret = (v == *optv);
            perthd_stats[tid].nr_hit++;
          } else {
            ret = hashmap->set(k, v);
            perthd_stats[tid].nr_miss++;
          }

          ret ? perthd_stats[tid].nr_succ++ : perthd_stats[tid].nr_err++;
        }
      }));
    }
    for (auto &thd : thds)
      thd.join();
    thds.clear();
    auto end = std::chrono::steady_clock::now();

    std::cout << "Finish executing workload in "
              << std::chrono::duration<double>(end - stt).count() << "s"
              << std::endl;

    for (int i = 0; i < kNumMutatorThds; i++) {
      stats.accum(perthd_stats[i]);
    }
    std::cout << "cache hit ratio: "
              << static_cast<float>(stats.nr_hit) /
                     (stats.nr_hit + stats.nr_miss)
              << std::endl;

    if (stats.nr_err == 0)
      std::cout << "Test passed!" << std::endl;
    else
      std::cout << "Get test failed! " << stats.nr_succ << " passed, "
                << stats.nr_err << " failed." << std::endl;

    std::cout << "Cooling down..." << std::endl;
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));

    return 0;
  }
};

void signalHandler(int signum) {
  // Let the process exit normally so that daemon_ can be naturally destroyed.
  exit(signum);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, signalHandler);

  CachebankTest test;
  test.init();
  for (int i = 0; i < 100; i++)
    test.run();
  test.finalize();
  return 0;
}

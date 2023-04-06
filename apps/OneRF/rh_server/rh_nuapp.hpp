#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <random>
#include <string>

#include "backend.hpp"
#include "fback.hpp"
#include "mysql.hpp"
#include "nuapp.hpp"
#include "query_types.hpp"
#include "semaphore.hpp"

// [midas cache]
#include "cache_manager.hpp"
#include "sync_kv.hpp"
constexpr static int kNumBuckets = 1 << 20;

namespace onerf {
struct SubSystem {
  std::string name;
  Semaphore cache_sem;
  std::unique_ptr<midas::SyncKV<kNumBuckets>> cache_client;

  Semaphore back_sem;
  std::unique_ptr<Backend> back_client;
  void *shadow_cache;

  SubSystem(std::string name, std::string type, int max_cache_conns,
            int max_back_conns);

private:
  midas::CachePool *pool_;
};

struct SizeGen {
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_real_distribution<float> real_dist;
  std::map<float, int64_t> cutoffs;
  std::vector<float> ordered;

  SizeGen();
  SizeGen(const std::map<std::string, float> &buckets);

  int64_t size();

private:
  void init(const std::map<std::string, float> &buckets);
};

struct Latency {
  int64_t latency;
  enum class Status { MISS, HIT, ERROR } status;
  std::string type;          // request type
  std::string critical_path; // critical path: only set iff type == "req" then
                             // slowest request type
};
using Results = std::vector<Latency>;

class RHController {
public:
  std::mutex measure_mtx;
  std::list<Latency> lat_measurements;
  // size_t max_nr_measures;

  // private:
  //   constexpr static size_t kMaxNrMeasures = 100 * 1000;
};

// App server with RobinHood
class RHAppServer : public AppServer {
public:
  RHAppServer();

  bool ParseRequest(const QueryLayerSlice &layers) override;

  bool ExecSubQuery(std::mutex &ret_mtx, std::vector<Latency> &ret_lats,
                    SubSystem *subsys, const std::string dep,
                    const std::vector<std::string> &urls,
                    const std::vector<std::int8_t> cacheables);

private:
  void init_subsystems();
  void report_latency();
  void process_queries();
  void report_hit_ratio();

  std::map<std::string, std::unique_ptr<SubSystem>> subs_;
  RHController rh_ctrl_;
};
} // namespace onerf
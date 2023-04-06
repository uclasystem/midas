#include <cassert>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>

#include "fback.hpp"
#include "mysql.hpp"
#include "rh_nuapp.hpp"

// [midas cache]
#include "cache_manager.hpp"
#include "sync_kv.hpp"

namespace onerf {

constexpr static int64_t kTimeout = 10ll * 1000 * 1000 * 1000; // 10s
constexpr static FBack::Param kDefaultFBParam{
    .Hardness = 100, .IdCount = 1, .SizeLower = 8, .SizeUpper = 17};
constexpr static bool kBypassCache = false;

SizeGen::SizeGen() : gen(rd()) {}
SizeGen::SizeGen(const std::map<std::string, float> &buckets) : gen(rd()) {
  init(buckets);
}

int64_t SizeGen::size() {
  assert(!ordered.empty());
  auto val = real_dist(gen);
  for (auto k : ordered) {
    if (val < k)
      return cutoffs[k];
  }
  return cutoffs[ordered.back()];
}

void SizeGen::init(const std::map<std::string, float> &buckets) {
  float cutoff = 0;
  for (auto &[k, v] : buckets) {
    cutoff += v;
    cutoffs[cutoff] = std::stoll(k);
    ordered.emplace_back(cutoff);
  }
}

SubSystem::SubSystem(std::string name_, std::string type, int max_cache_conns,
                     int max_back_conns)
    : name(name_), cache_sem(max_cache_conns), back_sem(max_back_conns) {
  auto cmanager = midas::CacheManager::global_cache_manager();
  if (!cmanager->create_pool(name)) {
    std::cerr << "Failed to create cache pool " << name << std::endl;
    exit(-1);
  }
  pool_ = cmanager->get_pool(name);
  pool_->update_limit(1ll * 1024 * 1024 * 1024); // 1GB
  cache_client = std::make_unique<midas::SyncKV<kNumBuckets>>(pool_);
  if (type == "fb") {
    FBack::Param kFBParam{
        .Hardness = 400, .IdCount = 1, .SizeLower = 7, .SizeUpper = 16};
    back_client = std::make_unique<FBack>(kFBParam, max_back_conns);
  } else if (type == "mysql") {
    constexpr static FBack::Param kMySqlParam{
        .Hardness = 425, .IdCount = 1, .SizeLower = 10, .SizeUpper = 19};
    back_client = std::make_unique<FBack>(kMySqlParam, max_back_conns);
    // NOTE: RobinHood didn't provide mysql config so we use FB to simulate it.
    // back_client = std::make_unique<MySqlBack>(33000);
  }
}

RHAppServer::RHAppServer() { init_subsystems(); }

bool RHAppServer::ExecSubQuery(std::mutex &lats_mtx,
                               std::vector<Latency> &ret_lats,
                               SubSystem *subsys, const std::string dep,
                               const std::vector<std::string> &urls,
                               const std::vector<int8_t> cacheables) {
  auto start_time = std::chrono::high_resolution_clock::now();
  bool has_error = false;
  if (!subsys)
    return false;
  std::map<std::string, int64_t> real_size;
  for (auto &url : urls) {
    // TODO: supposely we should convert url to lower case
    real_size[url] = -1;
  }
  std::vector<std::string> cache_queries;
  std::map<std::string, bool> cache_hits;
  std::vector<std::string> back_queries;
  for (auto &[k, _] : real_size) {
    cache_queries.emplace_back(dep + ":" + k);
  }
  auto fulfilled = 0;
  if (!kBypassCache && subsys->cache_client) { // cache query
    auto cache_client = subsys->cache_client.get();
    midas::kv_types::BatchPlug plug;
    cache_client->batch_stt(plug);
    for (auto &k : cache_queries) {
      std::string real_key = k.substr((dep + ":").length());
      auto cache_key = midas::kv_utils::make_key(k.c_str(), k.length());
      auto [value, vlen] = cache_client->bget_single(cache_key, plug);
      if (value) {
        cache_hits[real_key] = true;
        real_size[real_key] = vlen;
        fulfilled++;
        free(value);
      } else {
        back_queries.emplace_back(real_key);
      }
    }
    cache_client->batch_end(plug);
  } else {
    for (auto &k : cache_queries) {
      std::string real_key = k.substr((dep + ":").length());
      back_queries.emplace_back(real_key);
    }
  }
  auto hit_time = std::chrono::high_resolution_clock::now();
  auto miss_time = hit_time;
  // cache miss path
  if (!has_error && !back_queries.empty()) {
    std::map<std::string, Item> vals;
    subsys->back_sem.get();
    auto succ = subsys->back_client->Request(back_queries, vals);
    miss_time = std::chrono::high_resolution_clock::now();
    if (succ) {
      for (auto &[key, item] : vals) {
        real_size[key] = item.length();
        fulfilled++;
        // set into cache
        std::string cache_key = dep + ":" + key;
        auto cache_client = subsys->cache_client.get();
        cache_client->set(cache_key.c_str(), cache_key.length(), item.c_str(),
                          item.length());
      }
    } else {
      std::cerr << "Backend error " << dep << std::endl;
      has_error = true;
    }
    subsys->back_sem.put();
  }

  if (fulfilled != real_size.size())
    has_error = true;

  // calculate latency. RobinHood only
  int64_t hit_lat, miss_lat;
  Latency::Status status = Latency::Status::ERROR;
  if (has_error) {
    hit_lat = miss_lat = kTimeout;
  } else {
    hit_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(hit_time -
                                                                   start_time)
                  .count(); // ns
    miss_lat = std::chrono::duration_cast<std::chrono::nanoseconds>(miss_time -
                                                                    start_time)
                   .count();
    if (fulfilled == real_size.size())
      status = Latency::Status::HIT;
  }
  Latency lat{
      .latency = miss_lat, .status = status, .type = dep, .critical_path = ""};
  {
    std::unique_lock<std::mutex> ul(lats_mtx);
    ret_lats.emplace_back(lat);
  }

  for (auto &[key, item_size] : real_size) {
    auto cur_lat = miss_lat;
    auto cur_status = status;
    if (cache_hits[key]) {
      cur_lat = hit_lat;
      cur_status = Latency::Status::HIT;
    }
    Latency lat{.latency = cur_lat,
                .status = cur_status,
                .type = dep,
                .critical_path = ""};
    {
      std::unique_lock<std::mutex> ul(rh_ctrl_.measure_mtx);
      rh_ctrl_.lat_measurements.emplace_back(lat);
    }
    // TODO: add to query sequence
  }
  return true;
}

bool RHAppServer::ParseRequest(const QueryLayerSlice &layers) {
  auto start_time = std::chrono::high_resolution_clock::now();
  Latency::Status req_state = Latency::Status::ERROR;
  std::string slowest_depname;
  int64_t slowest_latency = 0;
  for (auto &layer : layers) {
    std::vector<std::future<bool>> futures;
    std::mutex lats_mtx;
    std::vector<Latency> sq_lats;
    for (auto &[dep, queries] : layer) {
      auto iter = subs_.find(dep);
      if (iter == subs_.cend())
        continue; // skip
      auto subsys = iter->second.get();
      auto future =
          std::async(std::launch::async, [&, &dep = dep, &queries = queries] {
            return ExecSubQuery(lats_mtx, sq_lats, subsys, dep, queries.url_vec,
                                queries.cacheable_vec);
          });
      futures.emplace_back(std::move(future));
    }

    for (auto &future : futures) {
      future.get();
    }
    for (auto &query_result : sq_lats) {
      if (query_result.latency > slowest_latency) {
        slowest_latency = query_result.latency;
        slowest_depname = query_result.type;
      }
      if (query_result.status == Latency::Status::MISS)
        req_state = Latency::Status::MISS;
      else if (query_result.status == Latency::Status::HIT &&
               req_state != Latency::Status::MISS)
        req_state = Latency::Status::HIT;
    }
  }
  int64_t response_time = kTimeout;
  if (req_state != Latency::Status::MISS) {
    auto end_time = std::chrono::high_resolution_clock::now();
    response_time = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
  }
  {
    std::unique_lock<std::mutex> ul(rh_ctrl_.measure_mtx);
    Latency lat{.latency = response_time,
                .status = req_state,
                .type = "req",
                .critical_path = slowest_depname};
    rh_ctrl_.lat_measurements.emplace_back(lat);
  }

  return true;
}

void RHAppServer::init_subsystems() {
  int max_cache_conns = 100;
  int max_back_conns = 100;
  std::vector<std::string> fb_instances{
      "63956c27", "5b63fdf5", "812126d3", "64c1ce15", "df1794e4", "ac59f41b",
      "4607349c", "6a2ef110", "c1042784", "e8fc6018", "b02bdd0d"};
  std::vector<std::string> mysql_instances{"d6018659", "b4fbebd8", "7385c12d",
                                           "b293d37d", "9ee74b0b", "39f00c48",
                                           "e5fffc73", "1289b3bb", "30eaf8be"};

  for (auto &instance : fb_instances) {
    subs_[instance] = std::make_unique<SubSystem>(
        instance, "fb", max_cache_conns, max_back_conns);
  }
  for (auto &instance : mysql_instances) {
    subs_[instance] = std::make_unique<SubSystem>(
        instance, "mysql", max_cache_conns, max_back_conns);
  }
}
} // namespace onerf
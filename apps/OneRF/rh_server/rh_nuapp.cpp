#include <cassert>
#include <chrono>
#include <cstdint>
#include <future>
#include <iostream>
#include <mutex>
#include <random>

#include "fback.hpp"
#include "rh_nuapp.hpp"

namespace onerf {

constexpr static int64_t kTimeout = 10ll * 1000 * 1000 * 1000; // 10s
constexpr static FBack::Param kDefaultFBParam{
    .Hardness = 100, .IdCount = 1, .SizeLower = 8, .SizeUpper = 17};

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

SubSystem::SubSystem(std::string type, int max_cache_conns, int max_back_conns)
    : cache_sem(max_cache_conns), back_sem(max_back_conns) {
  if (type == "fb") {
    FBack::Param param;

    back_client = std::make_unique<FBack>(kDefaultFBParam, max_back_conns);
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
  // TODO: cache query
  auto hit_time = std::chrono::high_resolution_clock::now();
  auto miss_time = hit_time;
  for (auto &k : cache_queries) {
    std::string real_key = k.substr((dep + ":").length());
    back_queries.emplace_back(real_key);
  }
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
        // TODO: set into cache
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
  subs_["39f00c48"] =
      std::make_unique<SubSystem>("fb", max_cache_conns, max_back_conns);
}
} // namespace onerf
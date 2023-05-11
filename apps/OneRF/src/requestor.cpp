#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <random>
#include <ratio>
#include <thread>
#include <vector>

#include "query_types.hpp"
#include "requestor.hpp"
#include <nlohmann/json.hpp>

namespace onerf {
using json = nlohmann::json;

static inline uint64_t rdtscp() {
  uint32_t a, d, c;
  asm volatile("rdtscp" : "=a"(a), "=d"(d), "=c"(c));
  return ((uint64_t)a) | (((uint64_t)d) << 32);
}

static inline void json_to_request(json &json_data, QueryLayerSlice &request) {
  auto &json_qarray = json_data["d"];
  for (const json &query : json_qarray) {
    QueryLayer qlayer;
    for (const auto &[instance, vecs] : query.items()) {
      DepQuery dep_query;
      dep_query.size_vec = vecs["S"].get<std::vector<int64_t>>();
      dep_query.cacheable_vec = vecs["C"].get<std::vector<int8_t>>();
      dep_query.url_vec = vecs["U"].get<std::vector<std::string>>();
      qlayer[instance] = dep_query;
    }
    request.emplace_back(qlayer);
  }
}

Requestor::Requestor(AppServer *nuapp, const std::string &trace_file_name)
    : nuapp_(nuapp), trace_file_(trace_file_name) {
  init_timer();
  InitTrace();
  // InitReqTimes();
}

void Requestor::InitTrace() {
  std::ifstream input_file(trace_file_);
  if (!input_file) {
    std::cerr << "Failed to open input file!" << std::endl;
    exit(1);
  }

  int64_t trace_len = 0;
  std::string line;
  while (std::getline(input_file, line)) {
    json json_data;
    try {
      json_data = nlohmann::json::parse(line);
      QueryLayerSlice request;
      json_to_request(json_data, request);
      requests_.emplace_back(request);
      // std::cout << json_data.dump() << std::endl;
    } catch (const std::exception &e) {
      std::cerr << "Failed to parse input JSON data: " << e.what() << std::endl;
    }
    trace_len++;
  }

  std::cout << "Finish load trace! In total " << requests_.size() << " "
            << trace_len << std::endl;
}

void Requestor::Perf(double target_kops) {
  prepare_reqs(target_kops);
  std::cout << "Start warm up..." << std::endl;
  benchmark(all_warmup_reqs_);
  std::cout << "Done warm up" << std::endl;
  traces_ = std::move(benchmark(all_perf_reqs_));
  auto real_duration_us = std::accumulate(
      traces_.begin(), traces_.end(), 0ul, [](uint64_t ret, Trace t) {
        return std::max(ret, t.start_us + t.duration_us);
      });
  auto real_kops =
      static_cast<double>(traces_.size()) * 1000 / real_duration_us;
  std::cout << traces_.size() << std::endl;
  std::cout << "Target Tput: "
            << static_cast<double>(kExperimentReqs) * 1000 / real_duration_us
            << " KOps" << std::endl;
  std::cout << "Real Tput: " << real_kops << " KOps" << std::endl;
}

bool Requestor::init_timer() {
  auto stt_time = std::chrono::high_resolution_clock::now();
  uint64_t stt_cycles = rdtscp();
  volatile int64_t sum = 0;
  for (int i = 0; i < 10000000; i++) {
    sum += i;
  }
  uint64_t end_cycles = rdtscp();
  auto end_time = std::chrono::high_resolution_clock::now();
  uint64_t dur_ns =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - stt_time)
          .count();
  CPU_FREQ = (end_cycles - stt_cycles) * 1000 / dur_ns;
  std::cout << "Timer initialized, CPU Freq: " << CPU_FREQ << " MHz"
            << std::endl;
  return true;
}

inline uint64_t Requestor::microtime() { return rdtscp() / CPU_FREQ; }

std::vector<Trace>
Requestor::benchmark(std::vector<PerfReqWithTime> all_reqs[kNumPerfThds]) {
  std::vector<std::thread> thds;
  std::vector<Trace> all_traces[kNumPerfThds];

  for (int i = 0; i < kNumPerfThds; i++)
    all_traces[i].reserve(all_reqs[i].size());

  for (int i = 0; i < kNumPerfThds; i++) {
    thds.emplace_back([&, tid = i] {
      auto &reqs = all_reqs[tid];
      auto &traces = all_traces[tid];
      // std::cout << tid << " reqs.size " << reqs.size() << std::endl;

      auto start_us = microtime();
      for (const auto &req : reqs) {
        auto relative_us = microtime() - start_us;
        if (req.start_us > relative_us) {
          std::this_thread::sleep_for(
              std::chrono::microseconds(req.start_us - relative_us));
        } else if (req.start_us + kMissDDL < relative_us) {
          continue;
        }
        Trace trace;
        trace.absl_start_us = microtime();
        trace.start_us = trace.absl_start_us - start_us;
        bool succ = nuapp_->ParseRequest(*req.req);
        trace.duration_us = microtime() - start_us - trace.start_us;
        if (succ)
          traces.emplace_back(trace);
      }
    });
  }

  for (auto &thd : thds)
    thd.join();

  std::vector<Trace> gathered_traces;
  for (int i = 0; i < kNumPerfThds; i++) {
    gathered_traces.insert(gathered_traces.end(), all_traces[i].begin(),
                           all_traces[i].end());
  }
  return gathered_traces;
}

void Requestor::prepare_reqs(double target_kops) {
  assert(kWarmupReqs + kExperimentReqs <= requests_.size());
  for (int i = 0; i < kNumPerfThds; i++) {
    all_warmup_reqs_[i].clear();
    all_perf_reqs_[i].clear();
  }

  std::vector<std::thread> thds;
  for (int i = 0; i < kNumPerfThds; i++) {
    thds.emplace_back([&, tid = i] {
      std::random_device rd;
      std::mt19937 gen(rd());

      { // warmup reqs
        std::exponential_distribution<double> d(target_kops / 1000 /
                                                kNumPerfThds); // ms -> us
        const auto chunk_size = (kWarmupReqs + kNumPerfThds - 1) / kNumPerfThds;
        const auto stt_req_idx = chunk_size * tid;
        const auto end_req_idx =
            std::min(stt_req_idx + chunk_size, kWarmupReqs);

        uint64_t cur_us = 0;
        for (int j = stt_req_idx; j < end_req_idx; j++) {
          PerfReqWithTime prwt;
          prwt.start_us = cur_us;
          prwt.req = &requests_[j];
          auto interval = std::max(1l, std::lround(d(gen)));
          cur_us += interval;
          all_warmup_reqs_[tid].emplace_back(prwt);
        }
      }
      { // perf reqs
        std::exponential_distribution<double> d(target_kops / 1000 /
                                                kNumPerfThds); // ms -> us
        const auto chunk_size =
            (kExperimentReqs + kNumPerfThds - 1) / kNumPerfThds;
        const auto stt_req_idx = kWarmupReqs + chunk_size * tid;
        const auto end_req_idx =
            std::min(stt_req_idx + chunk_size, kWarmupReqs + kExperimentReqs);

        uint64_t cur_us = 0;
        for (int j = stt_req_idx; j < end_req_idx; j++) {
          PerfReqWithTime prwt;
          prwt.start_us = cur_us;
          prwt.req = &requests_[j];
          auto interval = std::max(1l, std::lround(d(gen)));
          cur_us += interval;
          all_perf_reqs_[tid].emplace_back(prwt);
        }
      }
    });
  }
  for (auto &thd : thds)
    thd.join();
}

} // namespace onerf
#include <chrono>
#include <fstream>
#include <iostream>

#include "query_types.hpp"
#include "requestor.hpp"
#include <nlohmann/json.hpp>
#include <random>
#include <thread>

namespace onerf {
using json = nlohmann::json;

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
  InitTrace();
  InitReqTimes();
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

void Requestor::InitReqTimes() {
  constexpr static int64_t us = 1000 * 1000;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::exponential_distribution<> warmup_dist(kWarmupLambda / us);
  std::exponential_distribution<> exp_dist(kExperimentLambda / us);

  auto nr_requests = requests_.size();
  for (int i = 0; i < nr_requests; i++) {
    if (i < kWarmupReqs) {
      int64_t delay = std::ceil(warmup_dist(gen));
      req_times.push_back(delay);
    } else {
      int64_t delay = std::ceil(exp_dist(gen));
      req_times.push_back(delay);
    }
  }
}

void Requestor::Perf() {
  auto stt_time = std::chrono::high_resolution_clock::now();
  auto nr_requests = requests_.size();
  for (int i = 0; i < nr_requests; i++) {
    // std::cout << request_str(request) << std::endl;
    // std::cout << "sleep for " << req_times[i] << std::endl;
    std::this_thread::sleep_for(std::chrono::microseconds(req_times[i]));
    nuapp_->ParseRequest(requests_[i]);
    if (i >= 10000)
      break;
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  std::cout << std::chrono::duration_cast<std::chrono::milliseconds>(end_time -
                                                                     stt_time)
                   .count()
            << "ms" << std::endl;
}

} // namespace onerf
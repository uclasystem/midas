#pragma once

#include <memory>
#include <string>
#include <vector>
#include <thread>

#include "query_types.hpp"
#include "nuapp.hpp"

namespace onerf {
constexpr static int kNumPerfThds = 20;

struct PerfReqWithTime {
  QueryLayerSlice *req;
  uint64_t start_us;
};

struct Trace {
  uint64_t absl_start_us;
  uint64_t start_us;
  uint64_t duration_us;
};

class Requestor {
public:
  Requestor(AppServer *nuapp,
            const std::string &trace_file_name = kTraceFileName);
  void InitTrace();
  void Perf(double target_kops);

private:
  bool init_timer();
  uint64_t microtime();

  void prepare_reqs(double target_kops);
  std::vector<Trace>
  benchmark(std::vector<PerfReqWithTime> all_reqs[kNumPerfThds]);
  std::vector<PerfReqWithTime> all_warmup_reqs_[kNumPerfThds];
  std::vector<PerfReqWithTime> all_perf_reqs_[kNumPerfThds];
  std::vector<Trace> traces_;

  std::vector<QueryLayerSlice> requests_;
  uint64_t CPU_FREQ;

  AppServer *nuapp_;
  std::string trace_file_;

  constexpr static int64_t kMissDDL = 10 * 1000 * 1000; // us
  constexpr static char kTraceFileName[] = "tracegen/trace.json";
  // constexpr static int64_t kWarmupDur = 10l * 1000 * 1000; // 10s
  // constexpr static int64_t kPerfDur = 300l * 1000 * 1000; // 300s
  constexpr static float kWarmupLambda = 200;
  constexpr static int64_t kWarmupReqs = 10000;
  constexpr static float kExperimentLambda = 400;
  constexpr static int64_t kExperimentReqs = 90000;
};
} // namespace onerf
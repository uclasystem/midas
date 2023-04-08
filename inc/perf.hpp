#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace midas {

constexpr static uint64_t to_us = 1000 * 1000;     // 1s = 10^6 us
constexpr static uint64_t kMissDDL = 100ul * to_us; // 100 s -> us

struct PerfRequest {
  virtual ~PerfRequest() = default;
};

struct PerfRequestWithTime {
  uint64_t start_us;
  std::unique_ptr<PerfRequest> req;
};

struct Trace {
  uint64_t absl_start_us;
  uint64_t start_us;
  uint64_t duration_us;
  Trace();
  Trace(uint64_t absl_start_us_, uint64_t start_us_, uint64_t duration_us_);
};

class PerfAdapter {
public:
  virtual std::unique_ptr<PerfRequest> gen_req(int tid) = 0;
  virtual bool serve_req(int tid, const PerfRequest *req) = 0;
};

// Closed-loop, possion arrival.
class Perf {
public:
  Perf(PerfAdapter &adapter);
  void reset();
  void run(uint32_t num_threads, double target_kops, uint64_t duration_us,
           uint64_t warmup_us = 0, uint64_t miss_ddl_thresh_us = kMissDDL);
  void run_phased(uint32_t num_threads, std::vector<double> target_kops_vec,
                  std::vector<uint64_t> &duration_us_vec,
                  std::vector<uint64_t> &transition_us_vec, double warmup_kops,
                  uint64_t warmup_us = 0,
                  uint64_t miss_ddl_thresh_us = kMissDDL);

  uint64_t get_average_lat();
  uint64_t get_nth_lat(double nth);
  std::vector<Trace> get_timeseries_nth_lats(uint64_t interval_us, double nth);
  double get_real_kops() const;
  const std::vector<Trace> &get_traces() const;

private:
  enum TraceFormat { kUnsorted, kSortedByDuration, kSortedByStart };

  PerfAdapter &adapter_;
  std::vector<Trace> traces_;
  TraceFormat trace_format_;
  double real_kops_;
  std::vector<float> tputs_;
  std::atomic_int32_t succ_ops;
  friend class Test;

  uint64_t gen_reqs(std::vector<PerfRequestWithTime> *all_reqs,
                    uint32_t num_threads, double target_kops,
                    uint64_t duration_us, uint64_t start_us = 0);
  uint64_t gen_phased_reqs(std::vector<PerfRequestWithTime> *all_reqs,
                           uint32_t num_threads,
                           std::vector<double> &target_kops_vec,
                           std::vector<uint64_t> &duration_us_vec,
                           std::vector<uint64_t> &transition_us_vec,
                           uint64_t start_us = 0);
  std::vector<Trace> benchmark(std::vector<PerfRequestWithTime> *all_reqs,
                               uint32_t num_threads,
                               uint64_t miss_ddl_thresh_us);

  void report_tput(uint64_t duration_us);
  void dump_tput();

  constexpr static bool kEnableReporter = true;
  constexpr static int kReportInterval = 5; // seconds
  constexpr static int kReportBatch = 10;  // update counter batch size
  constexpr static int kTransSteps = 10;
};
} // namespace midas
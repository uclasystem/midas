#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <random>
#include <thread>
#include <vector>
#include <fstream>

#include "logging.hpp"
#include "perf.hpp"
#include "time.hpp"

namespace midas {

Trace::Trace() : absl_start_us(0), start_us(0), duration_us(0) {}
Trace::Trace(uint64_t absl_start_us_, uint64_t start_us_, uint64_t duration_us_)
    : absl_start_us(absl_start_us_), start_us(start_us_),
      duration_us(duration_us_) {}

Perf::Perf(PerfAdapter &adapter)
    : adapter_(adapter), trace_format_(kUnsorted), real_kops_(0), succ_ops(0) {}

void Perf::reset() {
  traces_.clear();
  trace_format_ = kUnsorted;
  real_kops_ = 0;
  tputs_.clear();
}

uint64_t Perf::gen_reqs(std::vector<PerfRequestWithTime> *all_reqs,
                        uint32_t num_threads, double target_kops,
                        uint64_t duration_us, uint64_t start_us) {
  std::vector<std::thread> threads;

  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back([&, &reqs = all_reqs[i], tid = i] {
      std::random_device rd;
      std::mt19937 gen(rd());
      std::exponential_distribution<double> d(target_kops / 1000 / num_threads);
      uint64_t cur_us = start_us;
      uint64_t end_us = cur_us + duration_us;

      while (cur_us < end_us) {
        auto interval = std::max(1l, std::lround(d(gen)));
        PerfRequestWithTime req_with_time;
        req_with_time.start_us = cur_us;
        req_with_time.req = adapter_.gen_req(tid);
        reqs.emplace_back(std::move(req_with_time));
        cur_us += interval;
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }
  return start_us + duration_us;
}

uint64_t Perf::gen_phased_reqs(std::vector<PerfRequestWithTime> *all_reqs,
                               uint32_t num_threads,
                               std::vector<double> &target_kops_vec,
                               std::vector<uint64_t> &duration_us_vec,
                               std::vector<uint64_t> &transition_us_vec,
                               uint64_t start_us) {
  const auto nr_phases = target_kops_vec.size();
  auto cur_us = start_us;
  for (uint32_t phase = 0; phase < nr_phases; phase++) {
    auto target_kops = target_kops_vec[phase];
    auto duration_us = duration_us_vec[phase];
    cur_us = gen_reqs(all_reqs, num_threads, target_kops_vec[phase],
                      duration_us, cur_us);
    if (phase == nr_phases - 1)
      break;
    // transition phase
    const auto trans_dur_step_us = transition_us_vec[phase] / kTransSteps;
    const auto next_target_kops = target_kops_vec[phase + 1];
    const auto trans_step_kops = (next_target_kops - target_kops) / kTransSteps;
    auto transition_kops = target_kops;
    for (uint32_t step = 0; step < kTransSteps; step++) {
      cur_us = gen_reqs(all_reqs, num_threads, transition_kops,
                        trans_dur_step_us, cur_us);
      transition_kops += trans_step_kops;
    }
  }

  return cur_us;
}

std::vector<Trace> Perf::benchmark(std::vector<PerfRequestWithTime> *all_reqs,
                                   uint32_t num_threads,
                                   uint64_t miss_ddl_thresh_us) {
  std::vector<std::thread> threads;
  std::vector<Trace> all_traces[num_threads];

  for (uint32_t i = 0; i < num_threads; i++) {
    all_traces[i].reserve(all_reqs[i].size());
  }

  for (uint32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(
        [&, &reqs = all_reqs[i], &traces = all_traces[i], tid = i] {
          int nr_succ = 0;
          auto start_us = Time::get_us();

          for (const auto &req : reqs) {
            auto relative_us = Time::get_us() - start_us;
            if (req.start_us > relative_us) {
              std::this_thread::sleep_for(
                  std::chrono::microseconds(req.start_us - relative_us));
            } else if (req.start_us + miss_ddl_thresh_us < relative_us) {
              continue;
            }
            Trace trace;
            trace.absl_start_us = Time::get_us();
            trace.start_us = trace.absl_start_us - start_us;
            bool ok = adapter_.serve_req(tid, req.req.get());
            trace.duration_us = Time::get_us() - start_us - trace.start_us;
            if (ok) {
              traces.push_back(trace);
              nr_succ++;
              if (nr_succ % kReportBatch == 0) {
                succ_ops += nr_succ;
                nr_succ = 0;
              }
            }
          }
        });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::vector<Trace> gathered_traces;
  for (uint32_t i = 0; i < num_threads; i++) {
    gathered_traces.insert(gathered_traces.end(), all_traces[i].begin(),
                           all_traces[i].end());
  }
  return gathered_traces;
}

void Perf::run(uint32_t num_threads, double target_kops, uint64_t duration_us,
               uint64_t warmup_us, uint64_t miss_ddl_thresh_us) {
  std::vector<PerfRequestWithTime> all_warmup_reqs[num_threads];
  std::vector<PerfRequestWithTime> all_perf_reqs[num_threads];
  gen_reqs(all_warmup_reqs, num_threads, target_kops, warmup_us);
  gen_reqs(all_perf_reqs, num_threads, target_kops, duration_us);
  benchmark(all_warmup_reqs, num_threads, miss_ddl_thresh_us);

  bool stop = false;
  std::unique_ptr<std::thread> report_thd;
  if (kEnableReporter) {
    report_thd = std::make_unique<std::thread>([&] {
      while (!stop) {
        uint64_t duration_us = kReportInterval * to_us;
        std::this_thread::sleep_for(std::chrono::microseconds(duration_us));
        report_tput(duration_us);
      }
      dump_tput();
    });
  }

  traces_ =
      std::move(benchmark(all_perf_reqs, num_threads, miss_ddl_thresh_us));

  stop = true;
  if (report_thd)
    report_thd->join();

  auto real_duration_us =
      std::accumulate(traces_.begin(), traces_.end(), static_cast<uint64_t>(0),
                      [](uint64_t ret, Trace t) {
                        return std::max(ret, t.start_us + t.duration_us);
                      });
  real_kops_ = static_cast<double>(traces_.size()) / (real_duration_us / 1000);
}

void Perf::run_phased(uint32_t num_threads, std::vector<double> target_kops_vec,
                      std::vector<uint64_t> &duration_us_vec,
                      std::vector<uint64_t> &transition_us_vec,
                      double warmup_kops, uint64_t warmup_us,
                      uint64_t miss_ddl_thresh_us) {
  std::vector<PerfRequestWithTime> all_warmup_reqs[num_threads];
  std::vector<PerfRequestWithTime> all_perf_reqs[num_threads];
  auto cur_us = gen_reqs(all_warmup_reqs, num_threads, warmup_kops, warmup_us);
  gen_phased_reqs(all_perf_reqs, num_threads, target_kops_vec, duration_us_vec,
                  transition_us_vec, cur_us);

  benchmark(all_warmup_reqs, num_threads, miss_ddl_thresh_us);

  bool stop = false;
  std::unique_ptr<std::thread> report_thd;
  if (kEnableReporter) {
    report_thd = std::make_unique<std::thread>([&] {
      while (!stop) {
        uint64_t duration_us = kReportInterval * to_us;
        std::this_thread::sleep_for(std::chrono::microseconds(duration_us));
        report_tput(duration_us);
      }
      dump_tput();
    });
  }

  traces_ =
      std::move(benchmark(all_perf_reqs, num_threads, miss_ddl_thresh_us));

  stop = true;
  if (report_thd)
    report_thd->join();

  auto real_duration_us =
      std::accumulate(traces_.begin(), traces_.end(), static_cast<uint64_t>(0),
                      [](uint64_t ret, Trace t) {
                        return std::max(ret, t.start_us + t.duration_us);
                      });
  real_kops_ = static_cast<double>(traces_.size()) / (real_duration_us / 1000);
}

uint64_t Perf::get_average_lat() {
  if (trace_format_ != kSortedByDuration) {
    std::sort(traces_.begin(), traces_.end(),
              [](const Trace &x, const Trace &y) {
                return x.duration_us < y.duration_us;
              });
    trace_format_ = kSortedByDuration;
  }

  auto sum = std::accumulate(
      std::next(traces_.begin()), traces_.end(), 0ULL,
      [](uint64_t sum, const Trace &t) { return sum + t.duration_us; });
  return sum / traces_.size();
}

uint64_t Perf::get_nth_lat(double nth) {
  if (trace_format_ != kSortedByDuration) {
    std::sort(traces_.begin(), traces_.end(),
              [](const Trace &x, const Trace &y) {
                return x.duration_us < y.duration_us;
              });
    trace_format_ = kSortedByDuration;
  }

  size_t idx = nth / 100.0 * traces_.size();
  return traces_[idx].duration_us;
}

std::vector<Trace> Perf::get_timeseries_nth_lats(uint64_t interval_us,
                                                 double nth) {
  std::vector<Trace> timeseries;
  if (trace_format_ != kSortedByStart) {
    std::sort(
        traces_.begin(), traces_.end(),
        [](const Trace &x, const Trace &y) { return x.start_us < y.start_us; });
    trace_format_ = kSortedByStart;
  }

  auto cur_win_us = traces_.front().start_us;
  auto absl_cur_win_us = traces_.front().absl_start_us;
  std::vector<uint64_t> win_durations;
  for (auto &trace : traces_) {
    if (cur_win_us + interval_us < trace.start_us) {
      std::sort(win_durations.begin(), win_durations.end());
      if (win_durations.size() >= 100) {
        size_t idx = nth / 100.0 * win_durations.size();
        timeseries.emplace_back(absl_cur_win_us, cur_win_us,
                                win_durations[idx]);
      }
      cur_win_us += interval_us;
      absl_cur_win_us += interval_us;
      win_durations.clear();
    }
    win_durations.push_back(trace.duration_us);
  }

  return timeseries;
}

double Perf::get_real_kops() const { return real_kops_; }

const std::vector<Trace> &Perf::get_traces() const { return traces_; }

void Perf::report_tput(uint64_t duration_us) {
  float tput = succ_ops * 1000.0 / duration_us;
  MIDAS_LOG(kInfo) << "Tput: " << tput << " Kops";
  tputs_.emplace_back(tput);
  succ_ops = 0;
}

void Perf::dump_tput() {
  std::ofstream tput_file("tput.txt");
  if (!tput_file.good())
    return;
  for (auto tput : tputs_)
    tput_file << tput << "\t";
  tput_file << std::endl;
  tput_file.close();
}
} // namespace midas
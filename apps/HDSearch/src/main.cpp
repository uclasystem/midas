#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <thread>

#include "constants.hpp"
#include "feat_extractor.hpp"
#include "utils.hpp"

// [midas]
#include "perf.hpp"
#include "cache_manager.hpp"

namespace hdsearch {
float cache_ratio = 1.0;
size_t cache_size = (kSimulate ? kSimuNumImgs : 41620ull) * (80 + 8192);
} // namespace hdsearch

using namespace hdsearch;

void get_timeseries_nth_lats(midas::Perf &perf, double nth) {
  const auto slice_us = 5 * midas::to_us; // 5s
  std::string filename = "p";
  if (nth == 99.9)
    filename += "99.9";
  else
    filename += std::to_string(int(nth));
  filename += ".txt";
  std::ofstream nth_file(filename);
  assert(nth_file.good());
  auto timeseries = perf.get_timeseries_nth_lats(slice_us, nth);
  std::cout << "P" << nth << " time series: ";
  for (auto &ts : timeseries) {
    std::cout << ts.duration_us << " ";
    nth_file << ts.duration_us << "\n";
  }
  std::cout << std::endl;
  nth_file << std::endl;
  nth_file.close();
}

int const_workload(int argc, char *argv[]) {
  FeatExtractor client;
  client.warmup();

  for (int i = 0; i < 10; i++) {
    midas::Perf perf(client);
    auto target_kops = 20;
    auto duration_us = 10000 * midas::to_us;
    auto warmup_us = 10 * midas::to_us;
    auto miss_ddl = 10 * midas::to_us;
    perf.run(kNrThd, target_kops, duration_us, warmup_us, miss_ddl);
    auto real_kops = perf.get_real_kops();
    std::cout << real_kops << std::endl;
    std::cout << perf.get_nth_lat(50) << " " << perf.get_nth_lat(99) << " "
              << perf.get_nth_lat(99.9) << std::endl;
    get_timeseries_nth_lats(perf, 99);
    get_timeseries_nth_lats(perf, 99.9);
  }

  return 0;
}

int phase_workload(int argc, char *argv[]) {
  FeatExtractor client;
  client.warmup();

  midas::Perf perf(client);
  std::vector<double> target_kops_vec;
  std::vector<uint64_t> duration_us_vec;
  std::vector<uint64_t> transition_us_vec;
  for (int i = 0; i < 20; i++) {
    target_kops_vec.emplace_back((i + 1));
    duration_us_vec.emplace_back(600 * midas::to_us);
    transition_us_vec.emplace_back(100 * midas::to_us);
  }
  double warmup_kops = 10;
  uint64_t warmup_us = 10 * midas::to_us;
  perf.run_phased(kNrThd, target_kops_vec, duration_us_vec, transition_us_vec,
                  warmup_kops, warmup_us);
  // perf.run(kNrThd, 1.0, 10 * 1000 * 1000);
  auto real_kops = perf.get_real_kops();
  std::cout << real_kops << std::endl;
  std::cout << perf.get_nth_lat(50) << " " << perf.get_nth_lat(99) << " "
            << perf.get_nth_lat(99.9) << std::endl;
  get_timeseries_nth_lats(perf, 99);
  get_timeseries_nth_lats(perf, 99.9);

  return 0;
}

int stepped_workload(int argc, char *argv[]) {
  FeatExtractor client;
  client.warmup();

  midas::Perf perf(client);
  // warm up
  auto warmup_kops = 100;
  auto warmup_us = 5 * midas::to_us;
  perf.run(kNrThd, warmup_kops, 0, warmup_us);
  perf.reset();

  // perf
  std::vector<double> target_kops_vec{10, 11, 12, 14, 16, 18, 20};
  for (auto target_kops : target_kops_vec) {
    auto duration_us = 20 * midas::to_us;
    perf.reset();
    perf.run(kNrThd, target_kops, duration_us);

    auto real_kops = perf.get_real_kops();
    std::cout << "Target Kops: " << target_kops << ", Real Kops:" << real_kops << std::endl;
    std::cout << "Latency (us): " << perf.get_nth_lat(50) << " "
              << perf.get_nth_lat(99) << " " << perf.get_nth_lat(99.9)
              << std::endl;
    get_timeseries_nth_lats(perf, 99);
    get_timeseries_nth_lats(perf, 99.9);
  }

  return 0;
}

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::stof(argv[1]);
  midas::CacheManager::global_cache_manager()->create_pool(cachepool_name);
  auto pool =
      midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
  // pool->update_limit(cache_size * cache_ratio);
  pool->update_limit(5ll * 1024 * 1024 * 1024); // GB

  return const_workload(argc, argv);
  // return phase_workload(argc, argv);
  // return stepped_workload(argc, argv);
}
#include <cstdint>
#include <iostream>

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

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: ./" << argv[0] << " <cache ratio>" << std::endl;
    exit(-1);
  }
  cache_ratio = std::stof(argv[1]);
  midas::CacheManager::global_cache_manager()->create_pool(cachepool_name);
  auto pool =
      midas::CacheManager::global_cache_manager()->get_pool(cachepool_name);
  pool->update_limit(cache_size * cache_ratio);

  FeatExtractor client;

  midas::Perf perf(client);
  std::vector<double> target_kops_vec;
  std::vector<uint64_t> duration_us_vec;
  std::vector<uint64_t> transition_us_vec;
  for (int i = 0; i < 10; i++) {
    target_kops_vec.emplace_back(i + 1);
    duration_us_vec.emplace_back(10 * midas::to_us);
    transition_us_vec.emplace_back(10 * midas::to_us);
  }
  perf.run_phased(kNrThd, target_kops_vec, duration_us_vec, transition_us_vec,
                  10, 20 * midas::to_us);
  // perf.run(kNrThd, 1.0, 10 * 1000 * 1000);
  auto real_kops = perf.get_real_kops();
  std::cout << real_kops << std::endl;
  std::cout << perf.get_nth_lat(50) << " " << perf.get_nth_lat(99) << " "
            << perf.get_nth_lat(99.9) << std::endl;
  auto timeseries = perf.get_timeseries_nth_lats(1 * 1000 * 1000, 99);
  std::cout << "P99 time series: ";
  for (auto &ts : timeseries) {
    std::cout << ts.duration_us << " ";
  }
  std::cout << std::endl;
  std::cout << "Test passed!" << std::endl;

  return 0;
}
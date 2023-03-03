#include <algorithm>
#include <iostream>
#include <random>
#include <string>

#include "../inc/zipf.hpp"

using namespace std;

constexpr int64_t kNumEles = 64ll * 1024;
float kSkewness = 0.9;
constexpr int64_t kTolls = 320ll * 1024;

constexpr float kHitPenalty = 0.03;
constexpr float kMissPenalty = 15.0;

int main(int argc, char *argv[]) {
  if (argc <= 1) {
    std::cout << "Usage: ./missrate.bin <skewness>" << std::endl;
    exit(-1);
  } else {
    kSkewness = std::stof(argv[1]);
    std::cout << "skewness: " << kSkewness << std::endl;
  }

  std::random_device rd;
  std::mt19937 gen(rd());
  midas::zipf_table_distribution<> dist_zipf(kNumEles, kSkewness);

  std::vector<int> arr;
  for (int i = 0; i < kTolls; i++) {
    auto r = dist_zipf(gen) - 1;
    arr.push_back(r);
  }
  std::sort(arr.begin(), arr.end());

  std::vector<float> miss_rates;
  std::vector<float> tputs;
  int pos = 0;
  for (int i = 0; i < 10; i++) {
    int nr_cached = kNumEles * (i) / 10;
    while (pos < kTolls && arr[pos] < nr_cached)
      pos++;
    float miss_rate = 1 - pos * 1.0 / kTolls;
    // miss_rates.insert(miss_rates.begin(), miss_rate);
    miss_rates.push_back(miss_rate);
    tputs.push_back(1 /
                    (miss_rate * kMissPenalty + (1 - miss_rate) * kHitPenalty));
  }

  for (int i = 0; i < 10; i++)
    std::cout << i * 10 << "\t" << miss_rates[i] << "\t" << tputs[i]
              << std::endl;

  return 0;
}

#include <iostream>
#include <thread>
#include <vector>

#include "victim_cache.hpp"

static constexpr int kNumThds = 24;
static constexpr int kNumEntries = 20000;

int main(int argc, char *argv[]) {
  cachebank::VictimCache vc(10000, 10000);

  std::vector<std::thread> thds;
  std::vector<cachebank::VCEntry *> entries[kNumThds];
  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&, tid = i] {
      for (int j = 0; j < kNumEntries; j++) {
        auto entry = new cachebank::VCEntry();
        entries[tid].push_back(entry);
        vc.push_back(entry);
      }
    });
  }

  for (auto &thd : thds)
    thd.join();
  thds.clear();

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&, tid = i] {
      for (auto entry : entries[tid]) {
        vc.remove(entry);
      }
    });
  }

  for (auto &thd : thds)
    thd.join();
  thds.clear();

  std::cout << "Test passed!" << std::endl;

  return 0;
}
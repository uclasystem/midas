#include <iostream>
#include <thread>
#include <vector>

#include "object.hpp"
#include "victim_cache.hpp"

static constexpr int kNumThds = 24;
static constexpr int kNumEntries = 20000;

int main(int argc, char *argv[]) {
  midas::VictimCache vc(10000, 10000);

  std::vector<std::thread> thds;
  std::vector<midas::ObjectPtr *> optrs[kNumThds];
  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&, tid = i] {
      for (int j = 0; j < kNumEntries; j++) {
        auto optr = new midas::ObjectPtr();
        optrs[tid].push_back(optr);
        vc.put(optr, nullptr);
      }
    });
  }

  for (auto &thd : thds)
    thd.join();
  thds.clear();

  for (int i = 0; i < kNumThds; i++) {
    thds.emplace_back([&, tid = i] {
      for (auto optr : optrs[tid]) {
        vc.remove(optr);
      }
    });
  }

  for (auto &thd : thds)
    thd.join();
  thds.clear();

  std::cout << "Test passed!" << std::endl;

  return 0;
}
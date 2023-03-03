#include <iostream>
#include <thread>
#include <vector>

#include "resource_manager.hpp"
#include "utils.hpp"

constexpr int kNumThds = 10;
constexpr int kNumRegions = 10;

int main(int argc, char *argv[]) {
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThds; tid++) {
    thds.push_back(std::thread([]() {
      midas::ResourceManager *rmanager =
          midas::ResourceManager::global_manager();

      for (int i = 0; i < kNumRegions; i++) {
        rmanager->AllocRegion();
      }
      for (int i = 0; i < kNumRegions; i++) {
        rmanager->FreeRegions();
      }
    }));
  }

  for (auto &thd : thds)
    thd.join();
  return 0;
}
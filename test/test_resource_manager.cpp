#include <iostream>
#include <thread>
#include <vector>

#include "resource_manager.hpp"

constexpr int kNumThds = 10;
constexpr int kNumRegions = 10;

int main(int argc, char *argv[]) {
  std::vector<std::thread> thds;
  for (int tid = 0; tid < kNumThds; tid++) {
    thds.push_back(std::thread([]() {
      cachebank::ResourceManager *rmanager =
          cachebank::ResourceManager::global_manager();

      for (int i = 0; i < kNumRegions; i++) {
        rmanager->AllocRegion(cachebank::kPageChunkSize);
      }
      for (int i = 0; i < kNumRegions; i++) {
        rmanager->FreeRegion(cachebank::kPageChunkSize);
      }
    }));
  }

  for (auto &thd : thds)
    thd.join();
  return 0;
}
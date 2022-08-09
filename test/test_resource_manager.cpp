#include "resource_manager.hpp"

int main(int argc, char *argv[]) {
  cachebank::ResourceManager rmanager;

  for (int i = 0; i < 10; i++) {
    rmanager.AllocRegion(cachebank::kPageChunkSize);
  }
  for (int i = 0; i < 10; i++) {
    rmanager.FreeRegion(cachebank::kPageChunkSize);
  }
  return 0;
}
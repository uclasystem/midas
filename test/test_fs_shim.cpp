#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <numeric>

#include <stdio.h>

// #include "fs_shim.hpp"

constexpr static int32_t kNumEles = 100 * 1024 * 1024;
constexpr static int32_t kStride = 100;
constexpr static int32_t kRepeat = 4;
constexpr static char kFileName[] = "/tmp/test_fs_shim.data";

int *src_arr;
int *dst_arr;

std::atomic_int32_t nr_succ{0};
std::atomic_int32_t nr_fail{0};

int init() {
  src_arr = new int[kNumEles];
  dst_arr = new int[kNumEles];
  return 0;
}

int destory() {
  delete[] src_arr;
  delete[] dst_arr;
  return 0;
}

int prep() {
  std::memset(src_arr, 0, sizeof(int) * kNumEles);
  std::memset(src_arr, 0, sizeof(int) * kNumEles);
  static std::mt19937 mt(rand());
  static std::uniform_int_distribution<> dist(std::numeric_limits<int>::max());

  std::cout << "Prep..." << std::endl;
  for (int i = 0; i < kNumEles; i+= kStride)
    src_arr[i] = dist(mt);
  std::cout << "Prep done!" << std::endl;

  return 0;
}

int write() {
  std::ofstream outfile;
  outfile.open(kFileName);
  for(int i = 0; i < kNumEles; i++) {
    outfile << src_arr[i] << "\n";
  }
  outfile.close();

  std::cout << "Write done!" << std::endl;

  return 0;
}

int read() {
  std::ifstream infile;
  infile.open(kFileName);
  int cnt = 0;
  while(infile >> dst_arr[cnt]) {
    cnt++;
  }
  infile.close();

  if (cnt != kNumEles) {
    std::cerr << "Read failed! " << cnt << " out of " << kNumEles
              << " were read" << std::endl;
    nr_fail++;
  }

  for (int i = 0; i < cnt; i++) {
    if (dst_arr[i] != src_arr[i]) {
      std::cerr << "Check failed @ ele[" << i << "], " << dst_arr[i]
                << " != " << src_arr[i] << std::endl;
      nr_fail++;
      break;
    }
  }
  std::cout << "Read done!" << std::endl;

  return 0;
}

int main(int argc, char *argv[]) {
  init();
  for (int i = 0; i < kRepeat; i++) {
    prep();
    write();
    read();
    read();
    read();
  }
  destory();
  if (nr_fail == 0)
    std::cout << "Test passed!" << std::endl;
  else
    std::cout << "Test failed!" << std::endl;
  return 0;
}
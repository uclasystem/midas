#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>

#include "resilient_func.hpp"
#include "timer.hpp"

constexpr static int kBufLens[] = {
    1, 2, 3, 4, 6, 8, 16, 32, 53, 64, 71, 128, 199, 256, 512, 1024, 2048, 4096};

constexpr static int kPerfBufLen = 64;
constexpr static int kNumRepeat = 5000000;

void random_fill(char buf[], size_t len) {
    static std::random_device rd;
    static std::mt19937 mt(rd());
    static std::uniform_int_distribution<int> dist('A', 'z');

    for (uint32_t i = 0; i < len; i++) {
      buf[i] = dist(mt);
    }
}

bool is_same(const char *src, const char *dst, size_t len) {
  while (len-- > 0) {
    if (*(src++) != *(dst++)) {
      return false;
    }
  }
  return true;
}

void correctness() {
  int nr_test = 0;
  int nr_succ = 0;
  for (auto len : kBufLens) {
    char *src = reinterpret_cast<char *>(malloc(len));
    char *dst = reinterpret_cast<char *>(malloc(len));
    random_fill(src, len);
    auto ret = cachebank::rmemcpy(dst, src, len);
    // memcpy(dst, src, len);
    // auto ret = 1;
    if (!is_same(src, dst, len)) {
      std::cout << "memcpy(len = " << len << ") is wrong!" << std::endl;
    }
    free(src);
    free(dst);

    nr_succ += ret;
    nr_test++;
  }
  if (nr_succ == nr_test)
    std::cout << "Test passed! Correct." << std::endl;
  else
    std::cout << "Test failed! " << nr_succ << "/" << nr_test << " succeeded. "
              << std::endl;
}

void performance() {
  char *src = new char[kPerfBufLen];
  char **dsts = new char *[kNumRepeat];

  random_fill(src, kPerfBufLen);
  for (int i = 0; i < kNumRepeat; i++) {
    dsts[i] = new char[kPerfBufLen];
    memset(dsts[i], 0, kPerfBufLen);
  }

  {
    auto stt = cachebank::timer::timer();
    for (int i = 0; i < kNumRepeat; i++) {
      memcpy(dsts[i], src, kPerfBufLen);
    }
    auto end = cachebank::timer::timer();
    std::cout << "memcpy takes " << cachebank::timer::duration(stt, end)
              << " s" << std::endl;
  }

  {
    auto stt = cachebank::timer::timer();
    for (int i = 0; i < kNumRepeat; i++) {
      cachebank::rmemcpy(dsts[i], src, kPerfBufLen);
    }
    auto end = cachebank::timer::timer();
    std::cout << "rmemcpy takes " << cachebank::timer::duration(stt, end)
              << " s" << std::endl;
  }

  delete[] src;
  for (int i = 0; i < kNumRepeat; i++) {
    delete[] dsts[i];
  }
  delete[] dsts;
}

int main() {
  correctness();
  performance();

  return 0;
}
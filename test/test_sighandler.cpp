#include <iostream>

#include "sig_handler.hpp"
#include "timer.hpp"
#include "transient_ptr.hpp"
#include "utils.hpp"

constexpr static int kNumRepeat = 2'000'000;
constexpr static int kSize = 4096;

void do_work() {
  auto inv_addr = cachebank::kVolatileSttAddr + 0x100200300;
  uint8_t buf[kSize];
  cachebank::TransientPtr tptr(inv_addr, kSize);
  int nr_failed = 0;
  auto stt = cachebank::timer::timer();
  for (int i = 0; i < kNumRepeat; i++) {
    nr_failed += !tptr.copy_from(buf, kSize); // we expect false to be returned
    nr_failed += !tptr.copy_to(buf, kSize);
  }
  auto end = cachebank::timer::timer();
  auto dur = cachebank::timer::duration(stt, end);
  if (!nr_failed)
    std::cout << "Test passed! Duration: " << dur
              << "s, tput: " << kNumRepeat * 2 / dur << " ops" << std::endl;
  else
    std::cout << "Test failed! " << nr_failed << "/" << kNumRepeat * 2
              << " failed" << std::endl;
}

int main() {
  auto sig_handler = cachebank::SigHandler::global_sighandler();
  sig_handler->init();

  do_work();

  return 0;
}
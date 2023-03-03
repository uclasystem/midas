#include <iostream>

#include "sig_handler.hpp"
#include "utils.hpp"

class SoftPtr {
public:
  SoftPtr(bool valid = true) {
    if (valid) {
      ptr_ = new int[10];
    } else {
      // ptr_ = nullptr;
      ptr_ = reinterpret_cast<int *>(kInvPtr);
    }
    // std::cout << "SoftPtr(): ptr_ = " << ptr_ << std::endl;
  }
  ~SoftPtr() {
    // std::cout << "~SoftPtr(): ptr_ = " << ptr_ << std::endl;
    if (reinterpret_cast<int64_t>(ptr_) != kInvPtr && ptr_)
      delete[] ptr_;
  }

  void reset() { ptr_ = nullptr; }

  bool copy_from(void *src, size_t size, int64_t offset = 0);
  bool copy_to(void *dst, size_t size, int64_t offset = 0);
  // bool copy_from(void *src, size_t size, int64_t offset = 0);
  // bool copy_to(void *dst, size_t size, int64_t offset = 0);

private:
  constexpr static uint64_t kInvPtr = midas::kVolatileSttAddr + 0x100200300;
  int *ptr_;
};

bool SoftPtr::copy_from(void *src, size_t size, int64_t offset) {
  return midas::rmemcpy(ptr_, src, size);
}

bool SoftPtr::copy_to(void *dst, size_t size, int64_t offset) {
  return midas::rmemcpy(dst, ptr_, size);
}

void do_work() {
  SoftPtr sptr(false);
  int a = 10;
  bool ret = sptr.copy_from(&a, sizeof(int));
  std::cout << "copy_from returns " << ret << std::endl;
  ret = sptr.copy_to(&a, sizeof(int));
  std::cout << "copy_to   returns " << ret << std::endl;
  if (!ret)
    sptr.reset();
}

int main() {
  auto sig_handler = midas::SigHandler::global_sighandler();
  sig_handler->init();

  do_work();
  std::cout << "Test passed!" << std::endl;

  return 0;
}
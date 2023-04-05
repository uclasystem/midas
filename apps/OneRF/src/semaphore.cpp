#include <cassert>
#include <mutex>

#include "semaphore.hpp"

namespace onerf {
Semaphore::Semaphore(int capacity) : size_(0), capacity_(capacity) {}

void Semaphore::get() {
  std::unique_lock<std::mutex> ul(mtx_);
  while (size_ >= capacity_)
    cv_.wait(ul, [&] { return size_ < capacity_; });
  size_++;
}

void Semaphore::put() {
  std::unique_lock<std::mutex> ul(mtx_);
  bool full = size_ >= capacity_;
  if (full)
    assert(size_ == capacity_);
  size_--;
  ul.unlock();
  if (full)
    cv_.notify_one();
}
} // namespace onerf
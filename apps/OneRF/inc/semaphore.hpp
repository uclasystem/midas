#pragma once

#include <condition_variable>
#include <mutex>

namespace onerf {
class Semaphore {
public:
  Semaphore(int capacity);
  void get();
  void put();

private:
  int size_;
  int capacity_;

  std::mutex mtx_;
  std::condition_variable cv_;
};
} // namespace onerf

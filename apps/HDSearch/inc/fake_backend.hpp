#pragma once

#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "constants.hpp"
#include "utils.hpp"

namespace hdsearch {
class FakeBackend {
public:
  FakeBackend(int nr_processors = 2)
      : _nr_processors(nr_processors), _arrival_req_id(-1),
        _processed_req_id(-1), _alive(true) {
    for (int i = 0; i < _nr_processors; i++) {
      processor_thds.push_back(std::thread([&]() { processor(); }));
    }
  }
  ~FakeBackend() {
    _alive = false;
    {
      std::unique_lock<std::mutex> lk(_p_mtx);
      _p_cv.notify_all();
    }
    for (auto &thd : processor_thds)
      thd.join();
  }
  Feature *serve_req() {
    int req_id = 0;
    {
      std::unique_lock<std::mutex> plk(_p_mtx);
      req_id = _arrival_req_id.fetch_add(1) + 1;
    }
    _p_cv.notify_all();
    while (_processed_req_id.load() < req_id)
      std::this_thread::sleep_for(std::chrono::microseconds(100));
    auto ret = new Feature();
    return ret;
  }

private:
  int processor() {
    while (_alive) {
      {
        std::unique_lock<std::mutex> plk(_p_mtx);
        _p_cv.wait(plk, [&] {
          return !_alive || _arrival_req_id.load() > _processed_req_id.load();
        });
      }

      while (_arrival_req_id.load() > _processed_req_id.load()) {
        std::this_thread::sleep_for(
            std::chrono::microseconds(std::lround(kMissPenalty * 1000)));
        _processed_req_id.fetch_add(1);
      }
    }
    return 0;
  }

  int _nr_processors;

  std::mutex _p_mtx;
  std::condition_variable _p_cv;
  // std::mutex _c_mtx;
  // std::condition_variable _c_cv;

  std::atomic<int> _arrival_req_id;
  std::atomic<int> _processed_req_id;

  bool _alive;
  std::vector<std::thread> processor_thds;
};
} // namespace hdsearch
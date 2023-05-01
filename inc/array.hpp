#pragma once

#include <mutex>
#include <optional>

#include "cache_manager.hpp"
#include "object.hpp"

namespace midas {
template <typename T> class Array {
public:
  Array(int n);
  Array(CachePool *pool, int n);
  ~Array();
  std::unique_ptr<T> get(int idx);
  bool set(int idx, const T &t);

private:
  CachePool *pool_;
  ObjectPtr *data_;
  int len_;
};
} // namespace midas

#include "impl/array.ipp"
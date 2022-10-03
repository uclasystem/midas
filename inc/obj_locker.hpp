#pragma once

#include <cstdint>
#include <mutex>
#include <functional>

namespace cachebank {

class ObjLocker {
public:
  bool try_lock(uint64_t obj_addr);
  void lock(uint64_t obj_addr);
  void unlock(uint64_t obj_addr);

private:
  uint64_t hash_val(uint64_t);
  constexpr static uint32_t kNumMaps = 65536;
  std::mutex mtxes_[kNumMaps];
};
}; // namespace cachebank

#include "impl/obj_locker.ipp"

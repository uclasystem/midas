#pragma once

#include "obj_locker.hpp"
#include <cstdint>
namespace cachebank {

inline bool ObjLocker::try_lock(uint64_t obj_addr) {
  int bucket = hash_val(obj_addr) % kNumMaps;
  return mtxes_[bucket].try_lock();
}

inline void ObjLocker::lock(uint64_t obj_addr) {
  int bucket = hash_val(obj_addr) % kNumMaps;
  mtxes_[bucket].lock();
}

inline void ObjLocker::unlock(uint64_t obj_addr) {
  int bucket = hash_val(obj_addr) % kNumMaps;
  mtxes_[bucket].unlock();
}

inline uint64_t ObjLocker::hash_val(uint64_t input) {
  static auto hasher = std::hash<uint64_t>();
  return hasher(input);
}

} // namespace cachebank
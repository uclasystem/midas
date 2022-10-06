#pragma once

#include <memory>
#include <mutex>

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

inline ObjLocker *ObjLocker::global_objlocker() noexcept {
  static std::mutex _mtx;
  static std::shared_ptr<ObjLocker> locker_;
  if (locker_)
    return locker_.get();
  std::unique_lock<std::mutex> ul(_mtx);
  if (locker_)
    return locker_.get();
  locker_ = std::make_shared<ObjLocker>();
  return locker_.get();
}

} // namespace cachebank
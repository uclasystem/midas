#pragma once

#include <cstring>

#include "constants.hpp"
#include "utils.hpp"

#include <memory>
#include <mutex>
#include <sync_hashmap.hpp>

namespace std {
template <> struct hash<FeatExt::MD5Key> {
  size_t operator()(const FeatExt::MD5Key &k) const {
    return std::hash<std::string_view>()(
        std::string_view(k.data, FeatExt::kMD5Len));
  }
};

template <> struct equal_to<FeatExt::MD5Key> {
  size_t operator()(const FeatExt::MD5Key &k1,
                    const FeatExt::MD5Key &k2) const {
    return std::memcmp(k1.data, k2.data, FeatExt::kMD5Len) == 0;
  }
};
} // namespace std

namespace FeatExt {
constexpr static int kNumBuckets = 1 << 20;
using SyncHashMap = midas::SyncHashMap<kNumBuckets, MD5Key, Feature>;

SyncHashMap *global_hashmap() {
  static std::mutex mtx_;
  static std::unique_ptr<SyncHashMap> hash_map_;

  if (hash_map_)
    return hash_map_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (hash_map_)
    return hash_map_.get();
  hash_map_ = std::make_unique<SyncHashMap>();
  return hash_map_.get();
}
} // namespace FeatExt
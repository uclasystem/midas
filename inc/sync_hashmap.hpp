#pragma once

#include <mutex>
#include <vector>

namespace cachebank {

template <size_t NBuckets, typename Key, typename Tp,
          typename Hash = std::hash<Key>, typename Pred = std::equal_to<Key>,
          typename Alloc = std::allocator<std::pair<Key, Tp>>,
          typename Lock = std::mutex>
class SyncHashMap {
public:
  SyncHashMap();

  using Pair = std::pair<const Key, Tp>;
  template <typename K1> Tp *get(K1 &&key);
  template <typename K1, typename Tp1> bool set(K1 key, Tp1 v);
  template <typename K1> bool remove(K1 &&key);
  // std::vector<Pair> get_all_pairs();

private:
  struct BucketNode {
    uint64_t key_hash;
    Pair *pair;
    BucketNode *next;
  };
  Lock _locks[NBuckets];
  BucketNode _buckets[NBuckets];
};

} // namespace cachebank

#include "impl/sync_hashmap.ipp"
#pragma once

#include <cstring>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "cache_manager.hpp"
#include "log.hpp"
#include "object.hpp"

namespace midas {

template <size_t NBuckets, typename Key, typename Tp,
          typename Hash = std::hash<Key>, typename Pred = std::equal_to<Key>,
          typename Alloc = LogAllocator, typename Lock = std::mutex>
class SyncHashMap {
public:
  SyncHashMap();
  SyncHashMap(CachePool *pool);

  template <typename K1> std::unique_ptr<Tp> get(K1 &&key);
  template <typename K1> bool get(K1 &&key, Tp &v);
  template <typename K1, typename Tp1> bool set(const K1 &key, const Tp1 &v);
  template <typename K1> bool remove(K1 &&key);
  bool clear();
  // std::vector<Pair> get_all_pairs();

private:
  struct BucketNode {
    uint64_t key_hash;
    ObjectPtr pair;
    BucketNode *next;
  };
  using BNPtr = BucketNode *;

  template <typename K1, typename Tp1>
  BNPtr create_node(uint64_t key_hash, K1 &&k, Tp1 &&v);
  BNPtr delete_node(BNPtr *prev_next, BNPtr node);
  template <typename K1>
  bool iterate_list(uint64_t key_hash, K1 &&key, BNPtr *&prev_next,
                    BNPtr &node);
  template <typename K1> BNPtr *find(K1 &&key, bool remove = false);
  Lock locks_[NBuckets];
  BucketNode *buckets_[NBuckets];

  CachePool *pool_;
};

} // namespace midas

#include "impl/sync_hashmap.ipp"
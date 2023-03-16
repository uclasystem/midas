#pragma once

#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "robinhood.h"

#include "cache_manager.hpp"
#include "log.hpp"
#include "object.hpp"

namespace midas {

namespace ordered_set {
using Value = std::pair<void *, size_t>; // value buffer and its length
using ValueWithScore = std::pair<Value, double>; // value and its score
}

template <size_t NBuckets, typename Alloc = LogAllocator,
          typename Lock = std::mutex>
class SyncKV {
public:
  SyncKV();
  SyncKV(CachePool *pool);

  /** Basic Interfaces */
  void *get(const void *key, size_t klen, size_t *vlen);
  bool get(const void *key, size_t klen, void *value, size_t vlen);
  bool set(const void *key, size_t klen, const void *value, size_t vlen);
  bool remove(const void *key, size_t klen);
  bool clear();
  // std::vector<Pair> get_all_pairs();
  /** Ordered Set Interfaces */
  enum class UpdateType { // mimicing Redis-plus-plus
    EXIST,
    NOT_EXIST,
    ALWAYS
  };
  bool zadd(const void *key, size_t klen, const void *value, size_t vlen,
            double score, UpdateType type);
  bool zrange(const void *key, size_t klen, int64_t start, int64_t end,
              std::back_insert_iterator<std::vector<ordered_set::Value>> bi);
  bool zrevrange(const void *key, size_t klen, int64_t start, int64_t end,
                 std::back_insert_iterator<std::vector<ordered_set::Value>> bi);

private:
  struct BucketNode {
    uint64_t key_hash;
    ObjectPtr pair;
    BucketNode *next;
  };
  using BNPtr = BucketNode *;

  static inline uint64_t hash_(const void *key, size_t klen);
  void *get_(const void *key, size_t klen, void *value, size_t *vlen);
  BNPtr create_node(uint64_t hash, const void *k, size_t kn, const void *v,
                    size_t vn);
  BNPtr delete_node(BNPtr *prev_next, BNPtr node);
  bool iterate_list(uint64_t hash, const void *k, size_t kn, size_t *vn,
                    BNPtr *&prev_next, BNPtr &node);
  BNPtr *find(void *k, size_t kn, bool remove = false);
  Lock locks_[NBuckets];
  BucketNode *buckets_[NBuckets];

  CachePool *pool_;
};

} // namespace midas

#include "impl/sync_kv.ipp"
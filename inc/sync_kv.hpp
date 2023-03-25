#pragma once

#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "robinhood.h"

#include "cache_manager.hpp"
#include "construct_args.hpp"
#include "log.hpp"
#include "object.hpp"
#include "time.hpp"

namespace midas {

namespace kv_types {
using Key = std::pair<const void *, size_t>; // key location and its length
using Value = std::pair<void *, size_t>;     // value location and its length
using CValue = Key; // const value format is the same to key
using ValueWithScore = std::pair<Value, double>; // value and its score

struct BatchPlug {
  int16_t hits{0};
  int16_t misses{0};
  int16_t vhits{0};
  int16_t batch_size{0};

  inline void reset() { hits = misses = vhits = batch_size = 0; }
};
static_assert(sizeof(BatchPlug) <= sizeof(int64_t),
              "BatchPlug is not correctly aligned!");
} // namespace kv_types

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
              std::back_insert_iterator<std::vector<kv_types::Value>> bi);
  bool zrevrange(const void *key, size_t klen, int64_t start, int64_t end,
                 std::back_insert_iterator<std::vector<kv_types::Value>> bi);
  /** Batched Interfaces */
  int bget(std::vector<kv_types::Key> &keys,
           std::vector<kv_types::Value> &values);
  int bset(std::vector<kv_types::Key> &keys,
           std::vector<kv_types::CValue> &values);
  int bremove(std::vector<kv_types::Key> &keys);
  // User can also mark a section with batch_[stt|end] and manually bget_single
  void batch_stt(kv_types::BatchPlug &plug);
  int batch_end(kv_types::BatchPlug &plug);
  void *bget_single(const void *key, size_t klen, size_t *vlen,
                    kv_types::BatchPlug &plug);
  kv_types::Value bget_single(kv_types::Key key, kv_types::BatchPlug &plug);

private:
  struct BucketNode {
    uint64_t key_hash;
    ObjectPtr pair;
    BucketNode *next;
  };
  using BNPtr = BucketNode *;

  static inline uint64_t hash_(const void *key, size_t klen);
  void *get_(const void *key, size_t klen, void *value, size_t *vlen,
             kv_types::BatchPlug *plug, bool construct);
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
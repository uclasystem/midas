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
// using Key = std::pair<const void *, size_t>; // key location and its length
struct Key {
  const void *data;
  size_t size;
  Key() : data(nullptr), size(0) {}
  Key(const void *data_, size_t size_) : data(data_), size(size_) {}
};
static_assert(sizeof(Key) == sizeof(void *) + sizeof(size_t),
              "Key is not correctly aligned!");

// using Value = std::pair<void *, size_t>;     // value location and its length
struct Value {
  void *data;
  size_t size;
  Value() : data(nullptr), size(0) {}
  Value(void *data_, size_t size_) : data(data_), size(size_) {}
};
static_assert(sizeof(Value) == sizeof(void *) + sizeof(size_t),
              "Value is not correctly aligned!");

using CValue = Key; // const value format is the same to key
static_assert(sizeof(CValue) == sizeof(void *) + sizeof(size_t),
              "CValue is not correctly aligned!");

using ValueWithScore = std::pair<Value, double>; // value and its score
static_assert(sizeof(ValueWithScore) == sizeof(Value) + sizeof(double),
              "ValueWithScore is not correctly aligned!");

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

namespace kv_utils {
/** Util function to make a Key or [C]Value item.
 *    WARNING: clearly this declaration is very dirty and error-prune. Always
 * use the shortcuts defined below if possible. */
template <typename T> T make_(decltype(T::data) data, decltype(T::size) size);
/** Shortcuts to create certain key/value types. */
constexpr static auto make_key = make_<kv_types::Key>;
constexpr static auto make_value = make_<kv_types::Value>;
constexpr static auto make_cvalue = make_<kv_types::CValue>;
} // namespace kv_utils

template <size_t NBuckets, typename Alloc = LogAllocator,
          typename Lock = std::mutex>
class SyncKV {
public:
  SyncKV();
  SyncKV(CachePool *pool);
  ~SyncKV();

  /** Basic Interfaces */
  void *get(const void *key, size_t klen, size_t *vlen);
  kv_types::Value get(const kv_types::Key &key);
  template <typename K> kv_types::Value get(const K &&k);
  template <typename K, typename V> std::unique_ptr<V> get(const K &&k);
  bool get(const void *key, size_t klen, void *value, size_t vlen);

  bool set(const void *key, size_t klen, const void *value, size_t vlen);
  bool set(const kv_types::Key &key, const kv_types::CValue &value);
  template <typename K> bool set(const K &&k, const kv_types::CValue &value);
  template <typename K, typename V> bool set(const K &&k, const V &v);

  bool remove(const void *key, size_t klen);
  bool remove(const kv_types::Key &key);
  template <typename K> bool remove(const K &&k);

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
  int bget(const std::vector<kv_types::Key> &keys,
           std::vector<kv_types::Value> &values);
  template <typename K>
  int bget(const std::vector<K> &keys, std::vector<kv_types::Value> &values);
  template <typename K, typename V>
  int bget(const std::vector<K> &keys, std::vector<std::unique_ptr<V>> &values);

  int bset(const std::vector<kv_types::Key> &keys,
           const std::vector<kv_types::CValue> &values);
  template <typename K>
  int bset(const std::vector<K> &keys,
           const std::vector<kv_types::CValue> &values);
  template <typename K, typename V>
  int bset(const std::vector<K> &keys, const std::vector<V> &values);

  int bremove(const std::vector<kv_types::Key> &keys);
  template <typename K> int bremove(const std::vector<K> &keys);

  // User can also mark a section with batch_[stt|end] and manually bget_single
  void batch_stt(kv_types::BatchPlug &plug);
  int batch_end(kv_types::BatchPlug &plug);

  void *bget_single(const void *key, size_t klen, size_t *vlen,
                    kv_types::BatchPlug &plug);
  kv_types::Value bget_single(const kv_types::Key &key,
                              kv_types::BatchPlug &plug);
  template <typename K>
  kv_types::Value bget_single(const K &&k, kv_types::BatchPlug &plug);
  template <typename K, typename V>
  std::unique_ptr<V> bget_single(const K &&k, kv_types::BatchPlug &plug);

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
#pragma once

namespace midas {

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::SyncHashMap() {
  pool_ = CachePool::global_cache_pool();
  memset(buckets_, 0, sizeof(buckets_));
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::SyncHashMap(
    CachePool *pool)
    : pool_(pool) {
  memset(buckets_, 0, sizeof(buckets_));
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::~SyncHashMap() {
  clear();
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
std::unique_ptr<Tp>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::get(K1 &&k) {
  Tp *stored_v = reinterpret_cast<Tp *>(::operator new(sizeof(Tp)));
  if (get(std::forward<K1>(k), *stored_v))
    return std::unique_ptr<Tp>(stored_v);
  return nullptr;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::get(K1 &&k,
                                                                  Tp &v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  BNPtr node = buckets_[bucket_idx];
  bool found = false;
  while (node) {
    found = iterate_list(key_hash, k, prev_next, node);
    if (found)
      break;
  }
  if (!found) {
    lock.unlock();
    pool_->inc_cache_miss();
    goto failed;
  }
  assert(node);
  if (node->pair.null() || !node->pair.copy_to(&v, sizeof(Tp), sizeof(Key))) {
    if (node->pair.is_victim())
      pool_->inc_cache_victim_hit();
    node = delete_node(prev_next, node);
    lock.unlock();
    goto failed;
  }
  lock.unlock();
  pool_->inc_cache_hit();
  LogAllocator::count_access();
  return true;
failed:
  if (kEnableConstruct && pool_->get_construct_func()) {
    ConstructArgs args = {&k, sizeof(k), &v, sizeof(v)};
    ConstructPlug plug;
    pool_->construct_stt(plug);
    bool succ = pool_->construct(&args) == 0;
    if (!succ) // failed to re-construct
      return false;
    // successfully re-constructed
    set(k, v);
    pool_->construct_add(sizeof(v), plug);
    pool_->construct_end(plug);
    return succ;
  }
  return false;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::remove(K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  BNPtr node = buckets_[bucket_idx];
  bool found = false;
  while (node) {
    found = iterate_list(key_hash, k, prev_next, node);
    if (found)
      break;
  }
  if (!found) {
    lock.unlock();
    return false;
  }
  assert(node);
  delete_node(prev_next, node);
  lock.unlock();
  /* should not count access for remove() */
  // LogAllocator::count_access();
  return true;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1, typename Tp1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::set(
    const K1 &k, const Tp1 &v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  auto node = buckets_[bucket_idx];
  while (node) {
    auto found = iterate_list(key_hash, k, prev_next, node);
    if (found) {
      // Tp tmp_v = v;
      assert(sizeof(v) <= sizeof(Tp));
      // try to set in place
      if (!node->pair.null() &&
          node->pair.copy_from(&v, sizeof(Tp1), sizeof(Key))) {
        lock.unlock();
        LogAllocator::count_access();
        return true;
      } else {
        node = delete_node(prev_next, node);
        break;
      }
    }
  }

  auto new_node = create_node(key_hash, k, v);
  if (!new_node) {
    lock.unlock();
    return false;
  }
  *prev_next = new_node;
  lock.unlock();
  LogAllocator::count_access();
  return true;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::clear() {
  for (int idx = 0; idx < NBuckets; idx++) {
    auto &lock = locks_[idx];
    lock.lock();
    auto prev_next = &buckets_[idx];
    auto node = buckets_[idx];
    while (node)
      node = delete_node(prev_next, node);
    lock.unlock();
  }
  return true;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
using BNPtr =
    typename SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::BucketNode
        *;

/** Utility functions */
template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1, typename Tp1>
inline BNPtr<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::create_node(
    uint64_t key_hash, K1 &&k, Tp1 &&v) {
  // Tp tmp_v = v;
  assert(sizeof(k) <= sizeof(Key));
  assert(sizeof(v) <= sizeof(Tp));
  // auto allocator = pool_->get_allocator();

  auto *new_node = new BucketNode();
  if (!pool_->alloc_to(sizeof(Key) + sizeof(Tp), &new_node->pair) ||
      !new_node->pair.copy_from(&k, sizeof(Key)) ||
      !new_node->pair.copy_from(&v, sizeof(Tp1), sizeof(Key))) {
    delete new_node;
    return nullptr;
  }
  // assert(!new_node->pair.null());
  if (new_node->pair.null()) {
    MIDAS_LOG(kError) << "new node KV pair is freed!";
    delete new_node;
    return nullptr;
  }
  new_node->key_hash = key_hash;
  new_node->next = nullptr;
  return new_node;
}

/** remove bucket_node from the list */
// should always use as `node = delete_node()` when iterating the list
template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
inline BNPtr<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::delete_node(
    BNPtr *prev_next, BNPtr node) {
  assert(*prev_next == node);
  if (!node)
    return nullptr;
  auto next = node->next;

  // node->pair.free();
  // if (node->pair.is_victim())
  //   pool_->get_vcache()->remove(&node->pair);
  pool_->free(node->pair);
  delete node;

  *prev_next = next;
  return next;
}

/** return: <equal, valid> */
template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
inline bool
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::iterate_list(
    uint64_t key_hash, K1 &&k, BNPtr *&prev_next, BNPtr &node) {
  if (key_hash != node->key_hash) {
    prev_next = &(node->next);
    node = node->next;
    return false;
  }
  std::byte k_buf[sizeof(Key)];
  auto tmp_k = std::launder(reinterpret_cast<Key *>(&k_buf));
  if (node->pair.null() || !node->pair.copy_to(tmp_k, sizeof(Key))) {
    if (node->pair.is_victim())
      pool_->inc_cache_victim_hit();
    // prev remains the same when current node is deleted.
    node = delete_node(prev_next, node);
    return false;
  }
  if (!Pred()(k, *tmp_k)) {
    prev_next = &(node->next);
    node = node->next;
    return false;
  }
  return true;
}

} // namespace midas

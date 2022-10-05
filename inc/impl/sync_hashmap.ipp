#pragma once

namespace cachebank {

class SlabAllocator;

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::SyncHashMap() {
  memset(_buckets, 0, sizeof(_buckets));
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
std::unique_ptr<Tp>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::get(K1 &&k) {
  std::unique_ptr<Tp> stored_v =
      std::unique_ptr<Tp>(reinterpret_cast<Tp *>(::operator new(sizeof(Tp))));
  if (get(std::forward<K1>(k), *stored_v))
    return stored_v;
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

  auto &lock = _locks[bucket_idx];
  lock.lock();

  auto prev_next = &_buckets[bucket_idx];
  BNPtr node = _buckets[bucket_idx];
  bool found = false;
  while (node && node->pair.is_valid()) {
    found = iterate_list(key_hash, k, prev_next, node);
    if (found)
      break;
  }
  if (!found) {
    lock.unlock();
    return false;
  }
  assert(node);
  if (!node->pair.copy_to(&v, sizeof(Tp), sizeof(Key))) {
    delete_node(prev_next, node);
    lock.unlock();
    return false;
  }
  lock.unlock();
  return true;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::remove(K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = _locks[bucket_idx];
  lock.lock();

  auto prev_next = &_buckets[bucket_idx];
  BNPtr node = _buckets[bucket_idx];
  bool found = false;
  while (node && node->pair.is_valid()) {
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

  auto &lock = _locks[bucket_idx];
  lock.lock();

  auto prev_next = &_buckets[bucket_idx];
  auto node = _buckets[bucket_idx];
  while (node && node->pair.is_valid()) {
    auto found = iterate_list(key_hash, k, prev_next, node);
    if (found) {
      Tp tmp_v = v;
      // try to set in place
      if (node->pair.copy_from(&tmp_v, sizeof(Tp), sizeof(Key))) {
        lock.unlock();
        return true;
      } else {
        delete_node(prev_next, node);
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
  return true;
}

/** Utility functions */
template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1, typename Tp1>
inline BNPtr
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::create_node(
    uint64_t key_hash, K1 &&k, Tp1 &&v) {
  Tp tmp_v = v;
  auto allocator = LogAllocator::global_allocator();
  auto optptr = allocator->alloc(sizeof(Key) + sizeof(Tp));
  if (!optptr || !optptr->copy_from(&k, sizeof(Key)) ||
      !optptr->copy_from(&tmp_v, sizeof(Tp), sizeof(Key))) {
    return nullptr;
  }

  auto *new_node = new BucketNode();
  new_node->pair = *optptr;
  if (!new_node->pair.set_rref(reinterpret_cast<uint64_t>(&new_node->pair))) {
    delete new_node;
    return nullptr;
  }
  new_node->key_hash = key_hash;
  new_node->next = nullptr;
  return new_node;
}

/** remove bucket_node from the list */
template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
inline BNPtr
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::delete_node(
    BNPtr *prev_next, BNPtr node) {
  assert(*prev_next == node);
  if (!node)
    return nullptr;
  auto next = node->next;

  // LogAllocator::global_allocator()->free(node->pair);
  node->pair.free();
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
  std::aligned_storage_t<sizeof(Key), alignof(Key)> k_buf;
  auto tmp_k = std::launder(reinterpret_cast<Key *>(&k_buf));
  // Key tmp_k;
  if (!node->pair.copy_to(tmp_k, sizeof(Key))) {
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

} // namespace cachebank
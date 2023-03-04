#pragma once

namespace midas {

template <size_t NBuckets, typename Alloc, typename Lock>
SyncKV<NBuckets, Alloc, Lock>::SyncKV() {
  pool_ = CachePool::global_cache_pool();
  memset(buckets_, 0, sizeof(buckets_));
}

template <size_t NBuckets, typename Alloc, typename Lock>
SyncKV<NBuckets, Alloc, Lock>::SyncKV(CachePool *pool) : pool_(pool) {
  memset(buckets_, 0, sizeof(buckets_));
}

template <size_t NBuckets, typename Alloc, typename Lock>
bool SyncKV<NBuckets, Alloc, Lock>::get(const void *k, size_t kn, void *v,
                                        size_t vn) {
  auto key_hash = robin_hood::hash_bytes(k, kn);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  BNPtr node = buckets_[bucket_idx];
  bool found = false;
  while (node) {
    found = iterate_list(key_hash, k, kn, prev_next, node);
    if (found)
      break;
  }
  if (!found) {
    lock.unlock();
    pool_->inc_cache_miss();
    return false;
  }
  assert(node);
  if (node->pair.null() ||
      !node->pair.copy_to(v, vn, kn + sizeof(size_t) * 2)) {
    if (node->pair.is_victim())
      pool_->inc_cache_victim_hit();
    node = delete_node(prev_next, node);
    lock.unlock();
    return false;
  }
  lock.unlock();
  pool_->inc_cache_hit();
  LogAllocator::count_access();
  return true;
}

template <size_t NBuckets, typename Alloc, typename Lock>
bool SyncKV<NBuckets, Alloc, Lock>::remove(const void *k, size_t kn) {
  // auto hasher = Hash();
  // auto key_hash = hasher(k);
  auto key_hash = robin_hood::hash_bytes(k, kn);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  BNPtr node = buckets_[bucket_idx];
  bool found = false;
  while (node) {
    found = iterate_list(key_hash, k, kn, prev_next, node);
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

template <size_t NBuckets, typename Alloc, typename Lock>
bool SyncKV<NBuckets, Alloc, Lock>::set(const void *k, size_t kn, const void *v,
                                        size_t vn) {
  auto key_hash = robin_hood::hash_bytes(k, kn);
  auto bucket_idx = key_hash % NBuckets;

  auto &lock = locks_[bucket_idx];
  lock.lock();

  auto prev_next = &buckets_[bucket_idx];
  auto node = buckets_[bucket_idx];
  while (node) {
    auto found = iterate_list(key_hash, k, kn, prev_next, node);
    if (found) {
      size_t stored_vn = 0;
      if (node->pair.null() ||
          !node->pair.copy_to(&stored_vn, sizeof(size_t), sizeof(size_t)) ||
          vn > stored_vn /* cannot fit in place */) {
        node = delete_node(prev_next, node);
        break;
      }
      // try to set in place
      if (!node->pair.null() &&
          node->pair.copy_from(&vn, sizeof(size_t), sizeof(size_t)) &&
          node->pair.copy_from(v, vn, kn)) {
        lock.unlock();
        LogAllocator::count_access();
        return true;
      } else {
        node = delete_node(prev_next, node);
        break;
      }
    }
  }

  auto new_node = create_node(key_hash, k, kn, v, vn);
  if (!new_node) {
    lock.unlock();
    return false;
  }
  *prev_next = new_node;
  lock.unlock();
  LogAllocator::count_access();
  return true;
}

template <size_t NBuckets, typename Alloc, typename Lock>
bool SyncKV<NBuckets, Alloc, Lock>::clear() {
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

template <size_t NBuckets, typename Alloc, typename Lock>
using BNPtr = typename SyncKV<NBuckets, Alloc, Lock>::BucketNode *;

/** Utility functions */
template <size_t NBuckets, typename Alloc, typename Lock>
inline BNPtr<NBuckets, Alloc, Lock> SyncKV<NBuckets, Alloc, Lock>::create_node(
    uint64_t key_hash, const void *k, size_t kn, const void *v, size_t vn) {
  auto *new_node = new BucketNode();
  if (!pool_->alloc_to(sizeof(size_t) * 2 + kn + vn, &new_node->pair) ||
      !new_node->pair.copy_from(&kn, sizeof(size_t)) ||
      !new_node->pair.copy_from(&vn, sizeof(size_t), sizeof(size_t)) ||
      !new_node->pair.copy_from(k, kn, sizeof(size_t) * 2) ||
      !new_node->pair.copy_from(v, vn, kn + sizeof(size_t) * 2)) {
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
template <size_t NBuckets, typename Alloc, typename Lock>
inline BNPtr<NBuckets, Alloc, Lock>
SyncKV<NBuckets, Alloc, Lock>::delete_node(BNPtr *prev_next, BNPtr node) {
  assert(*prev_next == node);
  if (!node)
    return nullptr;
  auto next = node->next;

  pool_->free(node->pair);
  delete node;

  *prev_next = next;
  return next;
}

/** return: <equal, valid> */
template <size_t NBuckets, typename Alloc, typename Lock>
inline bool
SyncKV<NBuckets, Alloc, Lock>::iterate_list(uint64_t key_hash, const void *k,
                                            size_t kn, BNPtr *&prev_next,
                                            BNPtr &node) {
  if (key_hash != node->key_hash)
    goto notequal;
  else {
    size_t stored_kn = 0;
    if (node->pair.null() || !node->pair.copy_to(&stored_kn, sizeof(size_t)))
      goto faulted;
    if (stored_kn != kn)
      goto notequal;
    void *stored_k = malloc(kn);
    if (!node->pair.copy_to(stored_k, kn, sizeof(size_t) * 2)) {
      free(stored_k);
      goto faulted;
    }
    if (strncmp(reinterpret_cast<const char *>(k),
                reinterpret_cast<const char *>(stored_k), kn) != 0) {
      free(stored_k);
      goto notequal;
    }
  }
  return true;

faulted:
  if (node->pair.is_victim())
    pool_->inc_cache_victim_hit();
  // prev remains the same when current node is deleted.
  node = delete_node(prev_next, node);
  return false;
notequal:
  prev_next = &(node->next);
  node = node->next;
  return false;
}

} // namespace midas

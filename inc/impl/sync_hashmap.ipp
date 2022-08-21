#pragma once

#include "resource_manager.hpp"

namespace cachebank {

class SlabAllocator;

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::SyncHashMap() {
  for (size_t i = 0; i < NBuckets; i++) {
    _buckets[i].pair = nullptr;
    _buckets[i].next = nullptr;
  }
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
Tp *SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::get(K1 &&k) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  // return get_with_hash(std::forward<K1>(k), key_hash);

  auto equaler = Pred();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &_buckets[bucket_idx];
  auto &lock = _locks[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        auto ret = &pair->second;
        lock.unlock();
        return ret;
      }
    }
    bucket_node = bucket_node->next;
  }
  lock.unlock();
  return nullptr;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1, typename V1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::set(K1 k, V1 v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  // put_with_hash(std::move(k), std::move(v), key_hash);

  auto equaler = Pred();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &_buckets[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = _locks[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        // replace the value with the new one
        pair->second = std::forward<V1>(v);
        lock.unlock();
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }

  auto *allocator = ResourceManager::global_allocator();
  auto *pair = allocator->alloc<Pair>(1);
  if (!pair) {
    lock.unlock();
    return false;
  }
  new (pair) Pair(k, v);

  if (!prev_next) {
    bucket_node->key_hash = key_hash;
    bucket_node->pair = pair;
  } else {
    // auto *new_bucket_node = allocator->alloc<BucketNode>(1);
    // new (new_bucket_node) BucketNode();
    auto *new_bucket_node = new BucketNode();
    new_bucket_node->key_hash = key_hash;
    new_bucket_node->pair = pair;
    new_bucket_node->next = nullptr;
    *prev_next = new_bucket_node;
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
  // return remove_with_hash(std::forward<K1>(k), key_hash);

  auto equaler = Pred();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &_buckets[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto *allocator = ResourceManager::global_allocator();
  auto &lock = _locks[bucket_idx];
  lock.lock();

  while (bucket_node && bucket_node->pair) {
    if (key_hash == bucket_node->key_hash) {
      auto *pair = reinterpret_cast<Pair *>(bucket_node->pair);
      if (equaler(k, pair->first)) {
        if (!prev_next) {
          if (!bucket_node->next) {
            bucket_node->pair = nullptr;
          } else {
            auto *next = bucket_node->next;
            *bucket_node = *next;
            delete next;
          }
        } else {
          *prev_next = bucket_node->next;
          delete bucket_node;
        }
        lock.unlock();
        pair->~Pair();
        allocator->free(pair);
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
  }
  lock.unlock();
  return false;
}

} // namespace cachebank
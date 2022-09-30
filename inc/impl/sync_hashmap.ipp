#pragma once

#include <memory>

#include "log.hpp"

namespace cachebank {

class SlabAllocator;

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::SyncHashMap() {
  for (size_t i = 0; i < NBuckets; i++) {
    _buckets[i].pair.reset();
    _buckets[i].next = nullptr;
  }
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1>
std::unique_ptr<Tp>
SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::get(K1 &&k) {
  std::unique_ptr<Tp> stored_v =
      std::unique_ptr<Tp>(reinterpret_cast<Tp *>(::operator new(sizeof(Tp))));
  if (get(k, *stored_v))
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

  auto equaler = Pred();
  auto bucket_idx = key_hash % NBuckets;
  BucketNode **prev_next = nullptr;
  auto *bucket_node = &_buckets[bucket_idx];
  auto &lock = _locks[bucket_idx];

  std::aligned_storage_t<sizeof(Key), alignof(Key)> k_buf;
  auto stored_k = std::launder(reinterpret_cast<Key *>(&k_buf));
  lock.lock();
  while (bucket_node && bucket_node->pair.is_valid()) {
    if (key_hash == bucket_node->key_hash) {
      if (!bucket_node->pair.copy_to(stored_k, sizeof(Key))) {
        goto invalid;
      }
      if (equaler(k, *stored_k)) {
        if (bucket_node->pair.copy_to(&v, sizeof(Tp), sizeof(Key))) {
          lock.unlock();
          return true;
        } else {
          lock.unlock();
          return false;
        }
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
    continue;
  invalid:
    // remove bucket_node from the list
    if (!prev_next) {
      if (!bucket_node->next) {
        bucket_node->pair.reset();
      } else {
        auto *next = bucket_node->next;
        *bucket_node = *next;
        delete next;
      }
    } else {
      *prev_next = bucket_node->next;
      delete bucket_node;
      bucket_node = *prev_next;
    }
  }
  lock.unlock();
  return false;
}

template <size_t NBuckets, typename Key, typename Tp, typename Hash,
          typename Pred, typename Alloc, typename Lock>
template <typename K1, typename Tp1>
bool SyncHashMap<NBuckets, Key, Tp, Hash, Pred, Alloc, Lock>::set(K1 k, Tp1 v) {
  auto hasher = Hash();
  auto key_hash = hasher(k);
  // put_with_hash(std::move(k), std::move(v), key_hash);

  auto equaler = Pred();
  auto bucket_idx = key_hash % NBuckets;
  auto *bucket_node = &_buckets[bucket_idx];
  BucketNode **prev_next = nullptr;
  auto &lock = _locks[bucket_idx];
  auto *allocator = LogAllocator::global_allocator();

  std::aligned_storage_t<sizeof(Key), alignof(Key)> k_buf;
  auto stored_k = std::launder(reinterpret_cast<Key *>(&k_buf));
  lock.lock();
  while (bucket_node && bucket_node->pair.is_valid()) {
    if (key_hash == bucket_node->key_hash) {
      if (!bucket_node->pair.copy_from(&k_buf, sizeof(Key)))
        goto invalid;
      if (equaler(k, *stored_k)) { // find key successfully
        Tp stored_v(v);
        if (!bucket_node->pair.copy_from(&stored_v, sizeof(Tp), sizeof(Key)))
          goto invalid;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
    continue;
  invalid:
    // remove bucket_node from the list
    if (!prev_next) {
      if (!bucket_node->next) {
        bucket_node->pair.reset();
      } else {
        auto *next = bucket_node->next;
        *bucket_node = *next;
        delete next;
      }
    } else {
      *prev_next = bucket_node->next;
      delete bucket_node;
      bucket_node = *prev_next;
    }
  }

  auto optional_tptr = allocator->alloc(sizeof(Key) + sizeof(Tp));
  if (!optional_tptr || !optional_tptr->copy_from(&k, sizeof(Key)) ||
      !optional_tptr->copy_from(&v, sizeof(Tp), sizeof(Key))) {
    lock.unlock();
    return false;
  }

  if (!prev_next) {
    bucket_node->key_hash = key_hash;
    bucket_node->pair = *optional_tptr;
  } else {
    auto *new_bucket_node = new BucketNode();
    new_bucket_node->key_hash = key_hash;
    new_bucket_node->pair = *optional_tptr;
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
  auto *allocator = LogAllocator::global_allocator();
  auto &lock = _locks[bucket_idx];

  std::aligned_storage_t<sizeof(Key), alignof(Key)> k_buf;
  auto stored_k = std::launder(reinterpret_cast<Key *>(&k_buf));
  lock.lock();
  while (bucket_node && bucket_node->pair.is_valid()) {
    if (key_hash == bucket_node->key_hash) {
      auto pair = bucket_node->pair;
      if (!pair.copy_to(&k_buf, sizeof(Key)))
        goto invalid;
      if (equaler(k, *stored_k)) { // find key successfully
        if (!prev_next) {          // first bucket node
          if (!bucket_node->next) {
            bucket_node->pair.reset();
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
        allocator->free(pair);
        return true;
      }
    }
    prev_next = &bucket_node->next;
    bucket_node = bucket_node->next;
    continue;
  invalid:
    // remove bucket_node from the list
    if (!prev_next) {
      if (!bucket_node->next) {
        bucket_node->pair.reset();
      } else {
        auto *next = bucket_node->next;
        *bucket_node = *next;
        delete next;
      }
    } else {
      *prev_next = bucket_node->next;
      delete bucket_node;
      bucket_node = *prev_next;
    }
  }
  lock.unlock();
  return false;
}

} // namespace cachebank

#pragma once

namespace midas {
template <typename T> Array<T>::Array(int n) : len_(n) {
  pool_ = CachePool::global_cache_pool();
  data_ = new ObjectPtr[len_];
  assert(data_);
}

template <typename T>
Array<T>::Array(CachePool *pool, int n) : pool_(pool), len_(n) {
  data_ = new ObjectPtr[len_];
  assert(data_);
}

template <typename T> Array<T>::~Array() {
  if (data_) {
    for (int i = 0; i < len_; i++) {
      if (!data_[i].null())
        pool_->free(data_[i]);
    }
    delete[] data_;
    data_ = nullptr;
  }
}

template <typename T> std::unique_ptr<T> Array<T>::get(int idx) {
  if (idx >= len_)
    return nullptr;
  T *t = nullptr;
  auto &optr = data_[idx];
  if (optr.null())
    goto missed;

  t = reinterpret_cast<T *>(::operator new(sizeof(T)));
  if (!optr.copy_to(t, sizeof(T)))
    goto faulted;
  pool_->inc_cache_hit();
  pool_->get_allocator()->count_access();
  return std::unique_ptr<T>(t);

faulted:
  if (t)
    ::operator delete(t);
missed:
  if (optr.is_victim())
    pool_->inc_cache_victim_hit(&optr);
  pool_->inc_cache_miss();
  return nullptr;
}

template <typename T> bool Array<T>::set(int idx, const T &t) {
  if (idx >= len_)
    return false;
  auto &optr = data_[idx];
  auto allocator = pool_->get_allocator();
  if (!optr.null())
    pool_->free(optr);
  if (!allocator->alloc_to(sizeof(T), &optr) || !optr.copy_from(&t, sizeof(T)))
    return false;
  pool_->get_allocator()->count_access();
  return true;
}
} // namespace midas
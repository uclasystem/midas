#include "softmem.h"

#include "../../inc/cache_manager.hpp"
#include "../../inc/log.hpp"

ObjectPtr midas_alloc_soft(const CachePool pool, size_t size) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return reinterpret_cast<ObjectPtr>(nullptr);
  auto allocator = pool_->get_allocator();
  auto optr = new midas::ObjectPtr();
  if (!allocator->alloc_to(size, optr)) {
    delete optr;
    return reinterpret_cast<ObjectPtr>(nullptr);
  }
  return reinterpret_cast<ObjectPtr>(optr);
}

bool midas_free_soft(const CachePool pool, ObjectPtr optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  auto ret = pool_->free(*optr_);
  delete optr_;
  return ret;
}

bool midas_copy_from_soft(const CachePool pool, void *dest, const ObjectPtr src,
                          size_t len, int64_t offset) {
  auto optr = reinterpret_cast<midas::ObjectPtr *>(src);
  return optr->copy_to(dest, len, offset);
}

bool midas_copy_to_soft(const CachePool pool, ObjectPtr dest, const void *src,
                        size_t len, int64_t offset) {
  auto optr = reinterpret_cast<midas::ObjectPtr *>(dest);
  return optr->copy_from(src, len, offset);
}

bool midas_soft_ptr_null(const ObjectPtr optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->null();
}

bool midas_soft_ptr_is_victim(const ObjectPtr optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->is_victim();
}

bool midas_soft_ptr_contains(const ObjectPtr optr, const uint64_t addr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->contains(addr);
}
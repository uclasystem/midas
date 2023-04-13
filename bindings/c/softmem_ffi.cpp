#include "softmem.h"

#include "../../inc/object.hpp"
#include "../../inc/cache_manager.hpp"
#include "../../inc/log.hpp"

object_ptr_t midas_alloc_soft(const cache_pool_t pool, size_t size) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return reinterpret_cast<object_ptr_t>(nullptr);
  auto optr = new midas::ObjectPtr();
  if (!optr) {
    midas::MIDAS_LOG(midas::kError) << "Failed to allocate!";
    return reinterpret_cast<object_ptr_t>(nullptr);
  }
  auto allocator = pool_->get_allocator();
  if (!allocator->alloc_to(size, optr)) {
    delete optr;
    return reinterpret_cast<object_ptr_t>(nullptr);
  }
  return reinterpret_cast<object_ptr_t>(optr);
}

bool midas_free_soft(const cache_pool_t pool, object_ptr_t optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  auto ret = pool_->free(*optr_);
  delete optr_;
  return ret;
}

bool midas_copy_from_soft(const cache_pool_t pool, void *dest, const object_ptr_t src,
                          size_t len, int64_t offset) {
  auto optr = reinterpret_cast<midas::ObjectPtr *>(src);
  return optr->copy_to(dest, len, offset);
}

bool midas_copy_to_soft(const cache_pool_t pool, object_ptr_t dest, const void *src,
                        size_t len, int64_t offset) {
  auto optr = reinterpret_cast<midas::ObjectPtr *>(dest);
  return optr->copy_from(src, len, offset);
}

bool midas_soft_ptr_null(const object_ptr_t optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->null();
}

bool midas_soft_ptr_is_victim(const object_ptr_t optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->is_victim();
}

bool midas_soft_ptr_contains(const object_ptr_t optr, const uint64_t addr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  return optr_->contains(addr);
}
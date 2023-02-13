#include "softmem.h"

#include "../../inc/log.hpp"

ObjectPtr midas_alloc_soft(size_t size) {
  auto allocator = cachebank::LogAllocator::global_allocator();
  auto optr = new cachebank::ObjectPtr();
  if (!allocator->alloc_to(size, optr)) {
    delete optr;
    return reinterpret_cast<ObjectPtr>(nullptr);
  }
  return reinterpret_cast<ObjectPtr>(optr);
}

bool midas_free_soft(ObjectPtr optr) {
  auto optr_ = reinterpret_cast<cachebank::ObjectPtr *>(optr);
  auto ret = optr_->free();
  delete optr_;
  return ret == cachebank::RetCode::Succ;
}

bool midas_copy_from_soft(void *dest, const ObjectPtr src, size_t len,
                          int64_t offset) {
  auto optr = reinterpret_cast<cachebank::ObjectPtr *>(src);
  return optr->copy_to(dest, len, offset);
}

bool midas_copy_to_soft(ObjectPtr dest, const void *src, size_t len,
                        int64_t offset) {
  auto optr = reinterpret_cast<cachebank::ObjectPtr *>(dest);
  return optr->copy_from(src, len, offset);
}

bool midas_soft_ptr_null(const ObjectPtr optr) {
  auto optr_ = reinterpret_cast<cachebank::ObjectPtr *>(optr);
  return optr_->null();
}

bool midas_soft_ptr_contains(const ObjectPtr optr, const uint64_t addr) {
  auto optr_ = reinterpret_cast<cachebank::ObjectPtr *>(optr);
  return optr_->contains(addr);
}
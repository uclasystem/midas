#include "softmem.h"

#include "../../inc/object.hpp"
#include "../../inc/cache_manager.hpp"
#include "../../inc/log.hpp"

#include <list>
#include <mutex>

namespace midas {
namespace ffi {
constexpr static uint64_t kFreeListSize = 100000;
// static thread_local std::list<ObjectPtr *> local_free_list;
static std::list<ObjectPtr *> global_free_list;
static std::mutex fl_mtx;

static inline ObjectPtr *alloc_optr() {
  std::unique_lock<std::mutex> ul(fl_mtx);
  if (global_free_list.empty()) {
    for (int i = 0; i < kFreeListSize; i++) {
      global_free_list.emplace_back(new ObjectPtr());
    }
  }
  auto optr = global_free_list.front();
  global_free_list.pop_front();
  return optr;
}

static inline void free_optr(ObjectPtr *optr) {
  std::unique_lock<std::mutex> ul(fl_mtx);
  global_free_list.emplace_back(optr);
}

// static inline ObjectPtr *alloc_optr() {
//   if (local_free_list.empty()) {
//     std::unique_lock<std::mutex> ul(fl_mtx);
//     if (global_free_list.empty()) {
//       for (int i = 0; i < kFreeListSize; i++) {
//         local_free_list.emplace_back(new ObjectPtr());
//       }
//     } else {
//       auto it = global_free_list.begin();
//       uint64_t len = std::min(global_free_list.size(), kFreeListSize);
//       std::advance(it, len); // move all elements
//       local_free_list.splice(local_free_list.begin(), global_free_list,
//                              global_free_list.begin(), it);
//     }
//     assert(!local_free_list.empty());
//   }
//   auto optr = local_free_list.front();
//   local_free_list.pop_front();
//   return optr;
// }

// static inline void free_optr(ObjectPtr *optr) {
//   local_free_list.emplace_back(optr);
//   if (local_free_list.size() > kFreeListSize) {
//     auto it = local_free_list.begin();
//     uint64_t len = std::min(local_free_list.size(), kFreeListSize);
//     std::advance(it, len);

//     std::unique_lock<std::mutex> ul(fl_mtx);
//     global_free_list.splice(global_free_list.begin(), local_free_list,
//                             local_free_list.begin(), it);
//   }
// }
} // namespace ffi
} // namespace midas

object_ptr_t midas_alloc_soft(const cache_pool_t pool, size_t size) {
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  if (!pool_)
    return reinterpret_cast<object_ptr_t>(nullptr);
  // auto optr = new midas::ObjectPtr();
  auto optr = midas::ffi::alloc_optr();
  if (!optr) {
    midas::MIDAS_LOG(midas::kError) << "Failed to allocate!";
    return reinterpret_cast<object_ptr_t>(nullptr);
  }
  auto allocator = pool_->get_allocator();
  if (!allocator->alloc_to(size, optr)) {
    // delete optr;
    midas::ffi::free_optr(optr);
    return reinterpret_cast<object_ptr_t>(nullptr);
  }
  return reinterpret_cast<object_ptr_t>(optr);
}

bool midas_free_soft(const cache_pool_t pool, object_ptr_t optr) {
  auto optr_ = reinterpret_cast<midas::ObjectPtr *>(optr);
  auto pool_ = reinterpret_cast<midas::CachePool *>(pool);
  auto ret = pool_->free(*optr_);
  // delete optr_;
  midas::ffi::free_optr(optr_);
  return ret;
}

bool midas_copy_from_soft(const cache_pool_t pool, void *dest,
                          const object_ptr_t src, size_t len, int64_t offset) {
  auto optr = reinterpret_cast<midas::ObjectPtr *>(src);
  return optr->copy_to(dest, len, offset);
}

bool midas_copy_to_soft(const cache_pool_t pool, object_ptr_t dest,
                        const void *src, size_t len, int64_t offset) {
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
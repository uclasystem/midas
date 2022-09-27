#pragma once

#include <cstddef>

namespace cachebank {

class TransientPtr {
public:
  TransientPtr() = default;
  TransientPtr(void *ptr, size_t size);
  bool is_valid() const;
  bool set(void *ptr, size_t size); /* point to a transient addr. */
  /**
   * Ops with single transient reference (this).
   */
  bool copy_from(void *src, size_t len, size_t offset = 0);
  bool copy_to(void *dst, size_t len, size_t offset = 0);
  /**
   * Ops with two transient references (this & src/dst).
   */
  bool copy_from(TransientPtr &src, size_t len, size_t from_offset = 0,
                 size_t to_offset = 0);
  bool copy_to(TransientPtr &dst, size_t len, size_t from_offset = 0,
               size_t to_offset = 0);
  /**
   * Assign this pointer to dst. The location of dst is checked to make sure the
   * correct method is called.
   */
  bool assign_to_non_volatile(TransientPtr *dst);
  bool assign_to_local_region(TransientPtr *dst);
  bool assign_to_foreign_region(TransientPtr *dst);

private:
  void *ptr_;
  size_t size_; /* accessible range of the pointer */
};

} // namespace cachebank

#include "impl/transient_ptr.ipp"
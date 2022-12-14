#pragma once

#include <cstddef>
#include <cstdint>

// #define BOUND_CHECK

namespace cachebank {

class TransientPtr {
public:
  TransientPtr();
  TransientPtr(uint64_t addr, size_t size);
  bool null() const noexcept;
  bool set(uint64_t addr, size_t size); /* point to a transient addr. */
  bool reset() noexcept;
  TransientPtr slice(int64_t offset) const;
  TransientPtr slice(int64_t offset, size_t size) const;
  size_t size() const noexcept;
  /**
   * Atomic operations
   */
  bool cmpxchg(int64_t offset, uint64_t oldval, uint64_t newval);
  int64_t atomic_add(int64_t offset, int64_t val);
  /**
   * Ops with single transient reference (this).
   */
  bool copy_from(const void *src, size_t len, int64_t offset = 0);
  bool copy_to(void *dst, size_t len, int64_t offset = 0);
  /**
   * Ops with two transient references (this & src/dst).
   */
  bool copy_from(const TransientPtr &src, size_t len, int64_t from_offset = 0,
                 int64_t to_offset = 0);
  bool copy_to(TransientPtr &dst, size_t len, int64_t from_offset = 0,
               int64_t to_offset = 0);
  /**
   * Assign this pointer to dst. The location of dst is checked to make sure the
   * correct method is called.
   */
  bool assign_to_non_volatile(TransientPtr *dst);
  bool assign_to_local_region(TransientPtr *dst);
  bool assign_to_foreign_region(TransientPtr *dst);

  /**
   *  CAUTION! get raw address
   */
  uint64_t to_normal_address() const noexcept;

private:
  uint64_t ptr_;

  friend class ObjLocker;
#ifdef BOUND_CHECK
  size_t size_; /* accessible range of the pointer */
#endif          // BOUND_CHECK
};

} // namespace cachebank

#include "impl/transient_ptr.ipp"
#pragma once

#include "utils.hpp"

namespace cachebank {

inline void ObjectHdr::init() noexcept {
  size = kInvalidHdr;
  flags = kInvalidFlags;
}

inline void ObjectHdr::set(uint32_t size_) noexcept {
  size = size_;
  flags = 1 << kPresentBit;
}

inline void ObjectHdr::clr_present() noexcept {
  flags = clr_bit32(flags, kPresentBit);
}

inline void ObjectHdr::set_rref(uint64_t addr) noexcept { rref = addr; }

inline uint64_t ObjectHdr::get_rref() const noexcept { return rref; }

} // namespace cachebank
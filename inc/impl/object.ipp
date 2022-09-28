#pragma once

namespace cachebank {

inline void ObjectHdr::init() {
  size = kInvalidHdr;
  flags = kInvalidFlags;
}

inline void ObjectHdr::set(uint32_t size_) {
  size = size_;
  flags = 1 << kPresentBit;
}

inline void ObjectHdr::set_rref(uint64_t addr) { rref = addr; }

inline uint64_t ObjectHdr::get_rref() const { return rref; }

} // namespace cachebank
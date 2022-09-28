#pragma once

#include <cstdint>

namespace cachebank {

struct ObjectHdr {
  uint64_t rref; // reverse reference
  uint32_t size;
  uint32_t flags;

  void init();
  void set(uint32_t size_);

  void set_rref(uint64_t addr);
  uint64_t get_rref() const;

  constexpr static uint32_t kInvalidHdr = 0x1f1f1f1f;
  constexpr static uint32_t kInvalidFlags = 0x0;
  constexpr static uint32_t kPresentBit = 0;
  constexpr static uint32_t kAccessedBit = 1;
  constexpr static uint32_t kEvacuateBit = 2;
};

} // namespace cachebank

#include "impl/object.ipp"
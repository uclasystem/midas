#pragma once

#include "utils.hpp"

inline uint32_t SlabAllocator::get_slab_idx(uint32_t size) noexcept {
  uint32_t rounded_size = utils::round_up_power_of_two(size);
  assert(rounded_size <= kMaxSlabSize);
  return utils::bsr_32(std::max(rounded_size >> kMinSlabShift, 1u));
}

inline uint32_t SlabAllocator::get_slab_size(uint32_t idx) noexcept {
  return kMinSlabSize << idx;
}

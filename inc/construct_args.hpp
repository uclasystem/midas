#pragma once

#include <cstdint>

namespace midas {
struct ConstructArgs {
  const void *key;
  const uint64_t key_len;
  void *value;
  uint64_t value_len;
};
} // namespace midas
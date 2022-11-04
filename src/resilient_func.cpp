#include <cstddef>

#include "resilient_func.hpp"
#include "utils.hpp"

namespace cachebank {

// YIFAN: a very naive copy implementation
bool rmemcpy(void *dst, const void *src, size_t len) {
  for (int i = 0; i < len; i++) {
    reinterpret_cast<char *>(dst)[i] = reinterpret_cast<const char *>(src)[i];
  }
  return true;
}
DELIM_FUNC_IMPL(rmemcpy)

}
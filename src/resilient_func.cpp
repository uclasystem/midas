#include <cstddef>
#include <cstdint>
#include <immintrin.h>

#include "resilient_func.hpp"
#include "utils.hpp"

namespace cachebank {

// NOTE: must have len <= 8 bytes
static inline void rmemcpy_tiny(uint8_t *dst, const uint8_t *src, size_t len) {
  // assert(len <= 8); // should not assert in soft resilient functions
  if (LIKELY(len & 8)) {
    *(reinterpret_cast<uint64_t *>(dst)) =
        *(reinterpret_cast<const uint64_t *>(src));
    return;
  }
  if (len & 4) {
    *(reinterpret_cast<uint32_t *>(dst)) =
        *(reinterpret_cast<const uint32_t *>(src));
    dst += 4;
    src += 4;
  }
  if (len & 2) {
    *(reinterpret_cast<uint16_t *>(dst)) =
        *(reinterpret_cast<const uint16_t *>(src));
    dst += 2;
    src += 2;
  }
  if (len & 1)
    *dst = *src;
}

// NOTE: used when len <= 32 bytes (256 bits)
static inline void rmemcpy_small(void *dst, const void *src, size_t len) {
  auto *dst_ = reinterpret_cast<uint64_t *>(dst);
  const auto *src_ = reinterpret_cast<const uint64_t *>(src);
  auto qwords = len >> 3;
  len -= qwords << 3; // remaining len
  while (qwords-- > 0) {
    *(dst_++) = *(src_++);
  }
  if (UNLIKELY(len))
    rmemcpy_tiny(reinterpret_cast<uint8_t *>(dst_),
                 reinterpret_cast<const uint8_t *>(src_), len);
}

static inline void rmemcpy_ermsb(void *dst, const void *src, size_t len) {
  asm volatile("rep movsb" : "+D"(dst), "+S"(src), "+c"(len)::"memory");
}

static inline void rmemcpy_avx(void *dst, const void *src, size_t len) {
  /* dst, src -> 32 byte aligned
   * len -> multiple of 32 */
  auto *dst_vec = reinterpret_cast<__m256i *>(dst);
  const auto *src_vec = reinterpret_cast<const __m256i *>(src);

  size_t nr_vwords = len / sizeof(__m256i);
  len -= nr_vwords * sizeof(__m256i);
  for (; nr_vwords > 0; nr_vwords--, src_vec++, dst_vec++)
    _mm256_storeu_si256(dst_vec, _mm256_lddqu_si256(src_vec));

  if (len)
    rmemcpy_small(dst_vec, src_vec, len);
}

static inline void rmemcpy_avx_unroll(void *dst, const void *src, size_t len) {
  /* dst, src -> 128 byte aligned
   * len -> multiple of 128 */
  auto *dst_vec = reinterpret_cast<__m256i *>(dst);
  const auto *src_vec = reinterpret_cast<const __m256i *>(src);
  size_t nr_vwords = len / sizeof(__m256i);
  len -= nr_vwords * sizeof(__m256i);
  for (; nr_vwords > 0; nr_vwords -= 4, src_vec += 4, dst_vec += 4) {
    _mm256_storeu_si256(dst_vec + 0, _mm256_lddqu_si256(src_vec));
    _mm256_storeu_si256(dst_vec + 1, _mm256_lddqu_si256(src_vec + 1));
    _mm256_storeu_si256(dst_vec + 2, _mm256_lddqu_si256(src_vec + 2));
    _mm256_storeu_si256(dst_vec + 3, _mm256_lddqu_si256(src_vec + 3));
  }
  while (nr_vwords-- > 0) {
    _mm256_storeu_si256(dst_vec++, _mm256_lddqu_si256(src_vec++));
  }
  if (len) {
    rmemcpy_small(dst_vec, src_vec, len);
  }
}

// YIFAN: a very naive copy implementation
bool rmemcpy(void *dst, const void *src, size_t len) {
  if (src == dst || len == 0)
    return true;
  if (len <= 2 * sizeof(__m256i)) { // sizeof(__m256i) == 32
    rmemcpy_small(dst, src, len);
    return true;
  }
  if (len <= 2048) {
    rmemcpy_ermsb(dst, src, len);
    return true;
  }
  rmemcpy_avx_unroll(dst, src, len);
  return true;
}
DELIM_FUNC_IMPL(rmemcpy)

} // namespace cachebank
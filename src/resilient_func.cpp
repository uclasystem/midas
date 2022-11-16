#include <cstddef>
#include <cstdint>
#include <immintrin.h>

#include "resilient_func.hpp"
#include "utils.hpp"

namespace cachebank {

// Must have len < 16 bytes. Manually unroll instructions for data < 16 bytes.
FORCE_INLINE void rmemcpy_tiny(uint8_t *dst, const uint8_t *src, size_t len) {
  // assert(len < 16); // should not assert in soft resilient functions
  if (len & 8) {
    *(reinterpret_cast<uint64_t *>(dst)) =
        *(reinterpret_cast<const uint64_t *>(src));
    dst += 8;
    src += 8;
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
FORCE_INLINE void rmemcpy_small(void *dst, const void *src, size_t len) {
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

/** YIFAN: not perform well with small size (< 16 bytes) */
FORCE_INLINE void rmemcpy_ermsb(void *dst, const void *src, size_t len) {
  asm volatile("rep movsb" : "+D"(dst), "+S"(src), "+c"(len)::"memory");
}

FORCE_INLINE void rmemcpy_avx128(void *dst, const void *src, size_t len) {
  /* dst, src -> 16 bytes addresses
   * len -> divided into multiple of 16 */
  auto *dst_vec = reinterpret_cast<__m128i *>(dst);
  const auto *src_vec = reinterpret_cast<const __m128i *>(src);

  size_t nr_vwords = len / sizeof(__m128i);
  len -= nr_vwords * sizeof(__m128i);
  const bool dst_aligned = !((uint64_t)dst_vec & (0x80 - 1));
  const bool src_aligned = !((uint64_t)src_vec & (0x80 - 1));
  if (dst_aligned && src_aligned)
    for (; nr_vwords > 0; nr_vwords--, src_vec++, dst_vec++)
      _mm_store_si128(dst_vec, _mm_load_si128(src_vec));
  else
    for (; nr_vwords > 0; nr_vwords--, src_vec++, dst_vec++)
      _mm_storeu_si128(dst_vec, _mm_lddqu_si128(src_vec));

  if (len)
    rmemcpy_small(dst_vec, src_vec, len);
}

/** YIFAN: In my experience it is not faster than avx128 for most cases. */
FORCE_INLINE void rmemcpy_avx256(void *dst, const void *src, size_t len) {
  /* dst, src -> 32 bytes addresses
   * len -> divided into multiple of 32 */
  auto *dst_vec = reinterpret_cast<__m256i *>(dst);
  const auto *src_vec = reinterpret_cast<const __m256i *>(src);

  size_t nr_vwords = len / sizeof(__m256i);
  len -= nr_vwords * sizeof(__m256i);
  for (; nr_vwords > 0; nr_vwords--, src_vec++, dst_vec++)
    _mm256_storeu_si256(dst_vec, _mm256_lddqu_si256(src_vec));

  if (len)
    rmemcpy_small(dst_vec, src_vec, len);
}

/** YIFAN: In my experience it is not faster than avx128 for most cases. */
FORCE_INLINE void rmemcpy_avx_unroll(void *dst, const void *src, size_t len) {
  /* dst, src -> 256 byte aligned
   * len -> multiple of 256 */
  auto *dst_vec = reinterpret_cast<__m512i *>(dst);
  const auto *src_vec = reinterpret_cast<const __m512i *>(src);
  size_t nr_vwords = len / sizeof(__m512i);
  len -= nr_vwords * sizeof(__m512i);
  for (; nr_vwords > 0; nr_vwords -= 4, src_vec += 4, dst_vec += 4) {
    _mm512_storeu_si512(dst_vec + 0, _mm512_loadu_si512(src_vec));
    _mm512_storeu_si512(dst_vec + 1, _mm512_loadu_si512(src_vec + 1));
    _mm512_storeu_si512(dst_vec + 2, _mm512_loadu_si512(src_vec + 2));
    _mm512_storeu_si512(dst_vec + 3, _mm512_loadu_si512(src_vec + 3));
  }
  while (nr_vwords-- > 0) {
    _mm512_storeu_si512(dst_vec++, _mm512_loadu_si512(src_vec++));
  }
  if (len) {
    rmemcpy_small(dst_vec, src_vec, len);
  }
}

/**
 * YIFAN: within 20% overhead compared to std::memcpy for small data (< 16
 * bytes). ~30% faster than std::memcpy for large data (>= 512 bytes).
 */
bool rmemcpy(void *dst, const void *src, size_t len) {
  if (src == dst || len == 0)
    return true;
  if (UNLIKELY(len < sizeof(__m128i))) { // sizeof(__m128i) == 16
    rmemcpy_tiny(reinterpret_cast<uint8_t *>(dst),
                 reinterpret_cast<const uint8_t *>(src), len);
    return true;
  }
  rmemcpy_avx128(dst, src, len);
  return true;
}
DELIM_FUNC_IMPL(rmemcpy)

} // namespace cachebank
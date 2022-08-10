#include <cassert>
#include <cstddef>
#include <cstdint>
#include <sys/types.h>
#include <vector>

#include "resource_manager.hpp"
#include "utils.hpp"

namespace cachebank {

struct SlabHeader {
  uint32_t slab_id;
  uint32_t slab_size;
};

constexpr static uint32_t kAlignment = 8;
static_assert(sizeof(SlabHeader) % kAlignment == 0,
              "SlabHeader is not aligned correctly");

class SlabRegion : public VRange {
public:
  SlabRegion(uint32_t slab_id_, uint32_t slab_size_, void *addr,
             size_t region_size)
      : VRange(addr, region_size), slab_id(slab_id_), slab_size(slab_size_),
        capacity(region_size / slab_size -
                 1 /* the first element is for SlabHeader */),
        nr_alloced(capacity) {
    init();
  }
  void push(void *addr);
  void *pop();

  inline bool full() const noexcept { return nr_alloced == capacity; }
  inline uint32_t size() const noexcept { return nr_alloced; }

  using VRange::contains;

private:
  uint32_t init();
  const uint32_t slab_id;
  const uint32_t slab_size;
  const uint32_t capacity;
  uint32_t nr_alloced;
  struct FreeSlot {
    void *addr;
    FreeSlot *next;
  } * slots;
};

class SlabAllocator {
public:
  constexpr static uint8_t kNumSlabClasses = 8;
  // minial slab size should be 16 bytes for a FreeSlot (2 pointers)
  constexpr static uint32_t kMinSlabShift = 4;
  constexpr static uint32_t kMinSlabSize = (1 << kMinSlabShift);
  constexpr static uint32_t kMaxSlabSize =
      (kMinSlabSize << (kNumSlabClasses - 1));
  static_assert(sizeof(SlabHeader) <= kMinSlabSize,
                "SlabHeader is larger than kMinSlabSize");

  static void *alloc(uint32_t size);
  static void free(void *addr);

private:
  static inline uint32_t get_slab_idx(uint32_t size) noexcept;
  static inline uint32_t get_slab_size(uint32_t idx) noexcept;
  static thread_local std::vector<SlabRegion> slab_regions[kNumSlabClasses];
};

#include "impl/slab.ipp"

} // namespace cachebank
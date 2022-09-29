#include <cassert>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <sys/types.h>
#include <vector>

#include "resource_manager.hpp"
#include "utils.hpp"

namespace cachebank {

class SlabRegion;
struct SlabHeader {
  uint32_t slab_id;
  uint32_t slab_size;
  SlabRegion *region;
};

constexpr static uint32_t kAlignment = 16;
static_assert(sizeof(SlabHeader) % kAlignment == 0,
              "SlabHeader is not aligned correctly");

class SlabRegion : public VRange {
public:
  SlabRegion(uint32_t slab_id_, uint32_t slab_size_, void *addr,
             size_t region_size)
      : VRange(addr, region_size), slab_id(slab_id_), slab_size(slab_size_),
        capacity(region_size / slab_size -
                 1 /* the first element is for SlabHeader */),
        nr_alloced(capacity), nr_freed(0),
        ret_mtx(std::make_shared<std::mutex>()) {
    // TODO: lazily init to save memory
    init();
  }
  void push(void *addr);
  void *pop();

  inline bool full() const noexcept { return nr_alloced == capacity; }
  inline uint32_t size() const noexcept { return nr_alloced; }

  void evacuate(SlabRegion *dst) {}

  using VRange::contains;
  friend class SlabAllocator;

private:
  uint32_t init();
  const uint32_t slab_id;
  const uint32_t slab_size;
  const uint32_t capacity;
  uint32_t nr_alloced;
  uint32_t nr_freed;
  struct FreeSlot {
    void *addr;
    FreeSlot *next;
  };
  FreeSlot *slots;

  std::shared_ptr<std::mutex> ret_mtx;
  FreeSlot *ret_slots;
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

  void *alloc(uint32_t size);
  template <typename T> T *alloc(uint32_t cnt);
  void free(void *addr);

  void FreeRegion() {}

  static inline SlabAllocator *global_allocator();

private:
  void *_alloc(uint32_t size);
  static inline uint32_t get_slab_idx(uint32_t size) noexcept;
  static inline uint32_t get_slab_size(uint32_t idx) noexcept;
  static thread_local std::vector<std::shared_ptr<SlabRegion>>
      slab_regions[kNumSlabClasses];
};

} // namespace cachebank

#include "impl/slab.ipp"
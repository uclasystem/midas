#ifdef ENABLE_SLAB

#include <cstddef>
#include <iostream>

#include "logging.hpp"
#include "resource_manager.hpp"
#include "slab.hpp"
#include "utils.hpp"

namespace midas {

static inline SlabHeader *slab_header(const void *ptr) noexcept {
  uint64_t addr = reinterpret_cast<uint64_t>(ptr);
  return reinterpret_cast<SlabHeader *>(addr & kRegionMask);
}

uint32_t SlabRegion::init() {
  // init header
  SlabHeader *hdr = slab_header(stt_addr);
  hdr->slab_id = this->slab_id;
  hdr->slab_size = this->slab_size;
  hdr->region = this;
  // LOG(kDebug) << this;
  // init freelist
  uint32_t i;
  char *ptr = reinterpret_cast<char *>(stt_addr);
  ptr += slab_size;
  for (i = 0; i < capacity; i++, ptr += slab_size) {
    push(ptr);
  }

  return i;
}

inline void SlabRegion::push(void *addr) {
  assert(nr_alloced > 0);
  /* In place construct a FreeSlot */
  FreeSlot *slot = reinterpret_cast<FreeSlot *>(addr);
  /* YIFAN: this is actually unnecessary for the in-place slot list. Leave it
   * here in case later we change the design */
  slot->addr = addr;

  /* Push the new slot into the front */
  ret_mtx->lock();
  slot->next = ret_slots;
  ret_slots = slot;
  nr_freed++;
  ret_mtx->unlock();

  // LOG(kDebug) << "push " << addr;
}

inline void *SlabRegion::pop() {
  if (UNLIKELY(!slots || nr_alloced == capacity)) {
    if (UNLIKELY(!ret_slots))
      return nullptr;
    else { // slow path
      ret_mtx->lock();
      slots = ret_slots;
      ret_slots = nullptr;
      nr_alloced -= nr_freed;
      nr_freed = 0;
      ret_mtx->unlock();
    }
  }

  FreeSlot *slot = slots;
  slots = slot->next;

  /* Manual memset *slot to 0 */
  slot->addr = 0;
  slot->next = 0;

  nr_alloced++;
  return slot;
}

void *SlabAllocator::_alloc(uint32_t size) {
  uint32_t idx = get_slab_idx(size);
  uint32_t slab_size = get_slab_size(idx);
  assert(idx < kNumSlabClasses);

  // LOG(kDebug) << slab_size << " " << idx;

  for (auto &region : slab_regions[idx]) {
    // LOG(kDebug) << region.full();
    if (!region->full())
      return region->pop();
  }

  /* Slow path: allocate a new region */
  auto *rmanager = ResourceManager::global_manager();
  int rid = rmanager->AllocRegion();
  if (rid == -1)
    return nullptr;
  VRange range = rmanager->GetRegion(rid);
  slab_regions[idx].push_back(
      std::make_shared<SlabRegion>(idx, slab_size, range.stt_addr, range.size));

  return slab_regions[idx].back()->pop();
}

void SlabAllocator::free(void *addr) {
  /* cannot equal since the first slot is the slab header */
  assert(reinterpret_cast<int64_t>(addr) > kVolatileSttAddr);
  SlabHeader *hdr = slab_header(addr);
  auto *region = hdr->region;
  // LOG(kDebug) << region;
  region->push(addr);
}

thread_local std::vector<std::shared_ptr<SlabRegion>>
    SlabAllocator::slab_regions[kNumSlabClasses];

} // namespace midas

#endif // ENABLE_SLAB
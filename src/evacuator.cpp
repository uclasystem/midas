#include "evacuator.hpp"
#include "log.hpp"
#include "logging.hpp"
#include "resource_manager.hpp"
#include <memory>

namespace cachebank {

void Evacuator::evacuate() {
  auto allocator = LogAllocator::global_allocator();
  for (auto chunk : allocator->vLogChunks_) {
    auto raw_chunk_ptr = chunk.get();
    if (raw_chunk_ptr)
      evac_chunk(raw_chunk_ptr);
  }
}

void Evacuator::scan_chunk(LogChunk *chunk) { LOG(kError) << chunk->full(); }

void Evacuator::evac_chunk(LogChunk *chunk) { LOG(kError) << chunk->full(); }

} // namespace cachebank
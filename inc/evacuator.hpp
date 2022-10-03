#pragma once

#include "log.hpp"
#include "transient_ptr.hpp"

namespace cachebank {

class Evacuator {
public:
  void evacuate();
  void scan();

private:
  void evac_chunk(LogChunk *chunk);
  void scan_chunk(LogChunk *chunk);
};

} // namespace cachebank
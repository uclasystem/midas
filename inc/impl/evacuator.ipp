#pragma once

namespace cachebank {

inline Evacuator::Evacuator(int nr_thds) : nr_thds_(nr_thds), park_(false) {
  init();
}

inline void Evacuator::init() {}

} // namespace cachebank
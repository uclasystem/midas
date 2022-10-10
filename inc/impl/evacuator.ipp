#pragma once

namespace cachebank {

inline Evacuator::Evacuator() : nr_master_thd(0) { init(); }

inline void Evacuator::init() {}

} // namespace cachebank
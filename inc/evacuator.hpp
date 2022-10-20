#pragma once

#include <atomic>
#include <functional>
#include <mutex>

namespace cachebank {

class LogChunk;
class LogRegion;
class ObjectPtr;

class Evacuator {
public:
  Evacuator();
  // ~Evacuator();
  int64_t stw_gc(int64_t nr_to_reclaim);
  int64_t conc_gc(int nr_thds = kNumGCThds);
  void evacuate(int nr_thds = kNumGCThds);
  void scan(int nr_thds = kNumGCThds);

  static Evacuator *global_evacuator();

private:
  constexpr static int kNumGCThds = 48;

  void init();

  void scan_region(LogRegion *region, bool deactivate);
  bool evac_region(LogRegion *region);
  bool free_region(LogRegion *region);

  bool iterate_chunk(LogChunk *chunk, uint64_t &pos, ObjectPtr &optr);
  bool scan_chunk(LogChunk *chunk, bool deactivate);
  bool evac_chunk(LogChunk *chunk);
  bool free_chunk(LogChunk *chunk);

  template <class Iter, class T>
  void parallelizer(int nr_workers, Iter work_stt, Iter work_end,
                    std::function<bool(T)> fn);

  template <class C, class T>
  void parallelizer(int nr_workers, C &work, std::function<bool(T)> fn);

  int32_t under_pressure_;
  int32_t nr_gc_thds_;
  std::mutex mtx_;
};

} // namespace cachebank

#include "impl/evacuator.ipp"
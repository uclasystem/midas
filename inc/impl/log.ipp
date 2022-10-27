#pragma once

namespace cachebank {

/** LogChunk */
inline LogChunk::LogChunk(LogSegment *region, uint64_t addr)
    : alive_bytes_(0), region_(region), start_addr_(addr), pos_(addr),
      sealed_(false) {}

inline void LogChunk::seal() noexcept {
  if (full()) {
    sealed_ = true;
    return;
  }

  MetaObjectHdr endHdr;
  endHdr.set_invalid();
  auto endPtr = TransientPtr(pos_, sizeof(MetaObjectHdr));
  auto _ = endPtr.copy_from(&endHdr, sizeof(endHdr)); // ignore return value
  pos_ += sizeof(MetaObjectHdr);
  sealed_ = true;
}

inline bool LogChunk::full() noexcept {
  return sealed_ || pos_ + sizeof(MetaObjectHdr) > start_addr_ + kLogChunkSize;
}

inline void LogChunk::upd_alive_bytes(int32_t obj_size) noexcept {
  alive_bytes_ += obj_size;
  region_->alive_bytes_ += obj_size;
}

/** LogSegment */
inline LogSegment::LogSegment(int64_t rid, uint64_t addr)
    : alive_bytes_(0), region_id_(rid), start_addr_(addr), pos_(addr),
      sealed_(false), destroyed_(false) {}

inline void LogSegment::seal() noexcept { sealed_ = true; }

inline bool LogSegment::destroyed() const noexcept { return destroyed_; }

inline bool LogSegment::full() const noexcept {
  return pos_ >= start_addr_ + kRegionSize;
}

inline uint32_t LogSegment::size() const noexcept { return pos_ / kRegionSize; }

inline float LogSegment::get_alive_ratio() const noexcept {
  return static_cast<float>(alive_bytes_) / kRegionSize;
}

/** LogAllocator */
inline LogAllocator::LogAllocator() : curr_region_(0), curr_chunk_(0) {}

inline std::optional<ObjectPtr> LogAllocator::alloc(size_t size) {
  return alloc_(size, false);
}

inline bool LogAllocator::alloc_to(size_t size, ObjectPtr *dst) {
  auto optptr = alloc(size);
  if (!optptr)
    return false;
  auto &ptr = *optptr;
  *dst = ptr;
  ptr.set_rref(dst);
  return true;
}

inline bool LogAllocator::free(ObjectPtr &ptr) {
  return ptr.free() == RetCode::Succ;
}

inline int LogAllocator::cleanup_regions() {
  int reclaimed = 0;
  for (auto rit = vRegions_.begin(); rit != vRegions_.end();) {
    if ((*rit)->destroyed()) {
      rit = vRegions_.erase(rit);
      reclaimed++;
    } else
      rit++;
  }
  return reclaimed;
}

/* static functions */
inline int64_t LogAllocator::total_access_cnt() noexcept {
  return total_access_cnt_;
}

inline void LogAllocator::reset_access_cnt() noexcept { total_access_cnt_ = 0; }

inline int64_t LogAllocator::total_alive_cnt() noexcept {
  return total_alive_cnt_;
}

inline void LogAllocator::reset_alive_cnt() noexcept { total_alive_cnt_ = 0; }

inline void LogAllocator::count_access() {
  constexpr static int32_t kAccPrecision = 1024;
  access_cnt_++;
  if (UNLIKELY(access_cnt_ >= kAccPrecision)) {
    total_access_cnt_ += access_cnt_;
    access_cnt_ = 0;
  }
}

inline void LogAllocator::count_alive(int val) {
  constexpr static int32_t kAccPrecision = 1024;
  alive_cnt_ += val;
  if (UNLIKELY(alive_cnt_ >= kAccPrecision || alive_cnt_ <= -kAccPrecision)) {
    total_alive_cnt_ += alive_cnt_;
    alive_cnt_ = 0;
  }
}

inline void LogAllocator::thd_exit() {
  seal_pcab();

  if (access_cnt_) {
    total_access_cnt_ += access_cnt_;
    access_cnt_ = 0;
  }
}

inline void LogAllocator::seal_pcab() {
  if (pcab.get())
    pcab->seal();
}

/* A thread safe way to create a global allocator and get its reference. */
inline LogAllocator *LogAllocator::global_allocator() noexcept {
  static std::mutex mtx_;
  static std::unique_ptr<LogAllocator> _allocator(nullptr);

  if (LIKELY(_allocator.get() != nullptr))
    return _allocator.get();

  std::unique_lock<std::mutex> lk(mtx_);
  if (UNLIKELY(_allocator.get() != nullptr))
    return _allocator.get();

  _allocator = std::make_unique<LogAllocator>();
  return _allocator.get();
}

static thread_local struct ThreadExiter {
  ~ThreadExiter() { LogAllocator::thd_exit(); }
} exiter;

} // namespace cachebank
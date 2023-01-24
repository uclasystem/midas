#pragma once

namespace cachebank {

/** LogSegment */
inline LogSegment::LogSegment(int64_t rid, uint64_t addr)
    : alive_bytes_(kMaxAliveBytes), region_id_(rid), start_addr_(addr),
      pos_(addr), sealed_(false), destroyed_(false) {}

inline bool LogSegment::full() const noexcept {
  return sealed_ ||
         pos_ - start_addr_ + sizeof(MetaObjectHdr) >= kLogSegmentSize;
}

inline int32_t LogSegment::remaining_bytes() const noexcept {
  return start_addr_ + kLogSegmentSize - pos_;
}

inline void LogSegment::set_alive_bytes(int32_t alive_bytes) noexcept {
  alive_bytes_ = alive_bytes;
}

inline void LogSegment::seal() noexcept { sealed_ = true; }

inline bool LogSegment::sealed() const noexcept { return sealed_; }

inline bool LogSegment::destroyed() const noexcept { return destroyed_; }

inline uint32_t LogSegment::size() const noexcept { return pos_ / kRegionSize; }

inline float LogSegment::get_alive_ratio() const noexcept {
  return static_cast<float>(alive_bytes_) / kRegionSize;
}

/** SegmentList */
inline void SegmentList::push_back(std::shared_ptr<LogSegment> segment) {
  std::unique_lock<std::mutex> ul_(lock_);
  segments_.push_back(segment);
}

inline std::shared_ptr<LogSegment> SegmentList::pop_front() {
  std::unique_lock<std::mutex> ul_(lock_);
  if (segments_.empty())
    return nullptr;
  auto max_retry = segments_.size();
  int retry = 0;
  while (retry < max_retry) {
    auto segment = segments_.front();
    segments_.pop_front();
    retry++;
    if (!segment->sealed()) { // put in-used segment back to list
      segments_.push_back(segment);
    } else if (segment->destroyed()) {
      continue; // remove destroyed segment (this should never happen though)
    } else
      return segment;
  }
  return nullptr;
}

inline bool SegmentList::empty() const noexcept { return segments_.empty(); }

/** LogAllocator */
inline LogAllocator::LogAllocator() {}

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
  constexpr static int32_t kAccPrecision = 32;
  access_cnt_++;
  if (UNLIKELY(access_cnt_ >= kAccPrecision)) {
    total_access_cnt_ += access_cnt_;
    access_cnt_ = 0;
  }
}

inline void LogAllocator::count_alive(int val) {
  constexpr static int32_t kAccPrecision = 32;
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

  if (alive_cnt_) {
    total_alive_cnt_ += alive_cnt_;
    alive_cnt_ = 0;
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
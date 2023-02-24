#pragma once

#include <mutex>
namespace cachebank {
inline VictimCache::VictimCache(size_t size_limit, size_t cnt_limit)
    : size_limit_(size_limit), cnt_limit_(cnt_limit), size_(0), cnt_(0),
      entries_(nullptr) {}

inline VictimCache::~VictimCache() {
  std::unique_lock<std::mutex> ul(mtx_);
  while (cnt_ > 0) {
    remove_locked(entries_);
  }
}

inline void VictimCache::push_back(VCEntry *entry) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (cnt_ == 0) {
    assert(!entries_);
    entry->prev = entry->next = entry;
    entries_ = entry;
  } else {
    auto tail = entries_->prev;
    entry->next = tail->next;
    entry->prev = tail;
    tail->next = entry;
    entries_->prev = entry;
  }
  assert(!entry->optr.is_victim());
  entry->optr.set_victim(true);
  cnt_++;
  size_ += entry->optr.data_size_in_segment();
  while (size_ > size_limit_ || cnt_ > cnt_limit_) {
    remove_locked(entries_);
  }
}

inline VCEntry *VictimCache::pop_front() {
  std::unique_lock<std::mutex> ul(mtx_);
  return remove_locked(entries_);
}

inline VCEntry *VictimCache::remove_locked(VCEntry *entry) {
  assert(cnt_ > 0);
  if (cnt_ == 0)
    return nullptr;
  else if (cnt_ == 1) {
    assert(entries_ == entry);
    assert(entry->next == entry);
    assert(entry->prev == entry);
    entries_ = nullptr;
  } else {
    entry->prev->next = entry->next;
    entry->next->prev = entry->prev;
    if (entry == entries_)
      entries_ = entries_->next;
  }
  assert(entry->optr.is_victim());
  entry->optr.set_victim(false);
  entry->prev = entry->next = nullptr;
  cnt_--;
  size_ -= entry->optr.data_size_in_segment();
  return entry;
}

inline void VictimCache::remove(VCEntry *entry) {
  if (!entry->optr.is_victim())
    return;
  std::unique_lock<std::mutex> ul(mtx_);
  remove_locked(entry);
}

inline size_t VictimCache::size() const noexcept { return size_; }

inline size_t VictimCache::count() const noexcept { return cnt_; }

} // namespace cachebank
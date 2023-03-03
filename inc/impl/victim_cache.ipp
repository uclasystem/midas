#pragma once

namespace midas {
inline VCEntry::VCEntry()
    : optr(nullptr), construct_args(nullptr), prev(nullptr), next(nullptr) {}

inline VCEntry::VCEntry(ObjectPtr *optr_, void *construct_args_)
    : optr(optr_), construct_args(construct_args_), prev(nullptr),
      next(nullptr) {}

inline VictimCache::VictimCache(size_t size_limit, size_t cnt_limit)
    : size_limit_(size_limit), cnt_limit_(cnt_limit), size_(0), cnt_(0),
      entries_(nullptr) {}

inline VictimCache::~VictimCache() {
  std::unique_lock<std::mutex> ul(mtx_);
  while (cnt_ > 0) {
    auto entry = remove_locked(entries_);
    assert(entry);
    delete entry;
  }
}

inline bool VictimCache::push_back(ObjectPtr *optr_addr, void *construct_args) {
  std::unique_lock<std::mutex> ul(mtx_);
  if (optr_addr->is_victim())
    return false;
  optr_addr->set_victim(true);
  if (map_.find(optr_addr) != map_.cend())
    return false;
  auto entry = new VCEntry(optr_addr, construct_args);
  map_[optr_addr] = entry;
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
  cnt_++;
  size_ += entry->optr->data_size_in_segment();
  while (size_ > size_limit_ || cnt_ > cnt_limit_) {
    auto evict_entry = remove_locked(entries_);
    assert(evict_entry);
    delete evict_entry;
  }

  return true;
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
  assert(entry->optr->is_victim());
  entry->optr->set_victim(false);
  entry->prev = entry->next = nullptr;
  cnt_--;
  size_ -= entry->optr->data_size_in_segment();
  map_.erase(entry->optr);
  return entry;
}

inline bool VictimCache::remove(ObjectPtr *optr_addr) {
  if (!optr_addr->is_victim())
    return false;
  std::unique_lock<std::mutex> ul(mtx_);
  if (!optr_addr->is_victim())
    return false;
  auto iter = map_.find(optr_addr);
  if (iter == map_.cend())
    return false;
  remove_locked(iter->second);
  return true;
}

inline size_t VictimCache::size() const noexcept { return size_; }

inline size_t VictimCache::count() const noexcept { return cnt_; }

} // namespace midas
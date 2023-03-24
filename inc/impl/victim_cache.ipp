#pragma once

namespace midas {
inline VCEntry::VCEntry()
    : optr(nullptr), construct_args(nullptr), prev(nullptr), next(nullptr),
      size(0) {}

inline VCEntry::VCEntry(ObjectPtr *optr_, void *construct_args_)
    : optr(optr_), construct_args(construct_args_), prev(nullptr),
      next(nullptr), size(optr_->data_size_in_segment()) {}

inline void VCEntry::reset(ObjectPtr *optr_, void *construct_args_) noexcept {
  optr = optr_;
  size = optr_->data_size_in_segment();
  construct_args = construct_args_;
}

inline VictimCache::VictimCache(int64_t size_limit, int64_t cnt_limit)
    : size_limit_(size_limit), cnt_limit_(cnt_limit), size_(0), cnt_(0),
      entries_(nullptr) {}

inline VictimCache::~VictimCache() {
  std::unique_lock<std::mutex> ul(mtx_);
  while (cnt_ > 0)
    pop_front_locked();
}

inline int64_t VictimCache::size() const noexcept { return size_; }
inline int64_t VictimCache::count() const noexcept { return cnt_; }

inline bool VictimCache::push_back(ObjectPtr *optr_addr,
                                   void *construct_args) noexcept {
  if (!kEnableVictimCache)
    return true;

  std::unique_lock<std::mutex> ul(mtx_);
  if (map_.find(optr_addr) != map_.cend()) {
    auto entry = map_[optr_addr];
    size_ -= entry->size;
    MIDAS_LOG(kError) << "Replace an existing optr in victim cache "
                      << optr_addr << "prev size " << entry->size
                      << ", curr size " << optr_addr->data_size_in_segment();
    entry->reset(optr_addr, construct_args);
    size_ += entry->size;
    optr_addr->set_victim(true);
    return true;
  }

  auto entry = create_entry(optr_addr, construct_args);
  if (!entries_) {
    // we just created an entry but haven't added it into the list
    assert(cnt_ == 1);
    entry->prev = entry->next = entry;
    entries_ = entry;
  } else {
    auto tail = entries_->prev;
    entry->prev = tail;
    tail->next = entry;
    entry->next = entries_;
    entries_->prev = entry;
  }
  while (size_ > size_limit_ || cnt_ > cnt_limit_) {
    pop_front_locked();
  }

  return true;
}

inline bool VictimCache::remove(ObjectPtr *optr_addr) noexcept {
  if (!kEnableVictimCache)
    return true;

  std::unique_lock<std::mutex> ul(mtx_);
  auto iter = map_.find(optr_addr);
  if (iter == map_.cend())
    return false;
  remove_locked(iter->second);
  return true;
}

/** Util functions. All functions should be called with lock */
inline void VictimCache::pop_front_locked() noexcept {
  remove_locked(entries_);
}

inline void VictimCache::remove_locked(VCEntry *entry) noexcept {
  assert(entry);
  assert(cnt_ > 0);
  if (cnt_ == 1) {
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
  delete_entry(entry);
}

/** optr must not be in the victim cache when calling */
inline VCEntry *VictimCache::create_entry(ObjectPtr *optr_addr,
                                          void *construct_args) noexcept {
  optr_addr->set_victim(true);
  auto entry = new VCEntry(optr_addr, construct_args);
  map_[optr_addr] = entry;
  cnt_++;
  size_ += entry->size;
  return entry;
}

/** entry must be in the victim cache when calling */
inline void VictimCache::delete_entry(VCEntry *entry) noexcept {
  entry->optr->set_victim(false);
  cnt_--;
  size_ -= entry->size;
  map_.erase(entry->optr);
  delete entry;
}

} // namespace midas
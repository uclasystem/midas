#pragma once

namespace midas {
inline VCEntry::VCEntry() : optr(nullptr), construct_args(nullptr), size(0) {}

inline VCEntry::VCEntry(ObjectPtr *optr_, void *construct_args_)
    : optr(optr_), construct_args(construct_args_),
      size(optr_->data_size_in_segment()) {}

inline void VCEntry::reset(ObjectPtr *optr_, void *construct_args_) noexcept {
  optr = optr_;
  size = optr_->data_size_in_segment();
  construct_args = construct_args_;
}

inline VictimCache::VictimCache(int64_t size_limit, int64_t cnt_limit)
    : size_limit_(size_limit), cnt_limit_(cnt_limit), size_(0), cnt_(0) {}

inline VictimCache::~VictimCache() {
  std::unique_lock<std::mutex> ul(mtx_);
  map_.clear();
  entries_.clear();
}

inline int64_t VictimCache::size() const noexcept { return size_; }
inline int64_t VictimCache::count() const noexcept { return cnt_; }

inline bool VictimCache::put(ObjectPtr *optr_addr,
                             void *construct_args) noexcept {
  if (!kEnableVictimCache)
    return true;

  std::unique_lock<std::mutex> ul(mtx_);
  auto iter = map_.find(optr_addr);
  if (iter != map_.cend()) {
    auto entry = iter->second;
    size_ -= entry->size;
    MIDAS_LOG(kError) << "Replace an existing optr in victim cache "
                      << optr_addr << ", prev size " << entry->size
                      << ", curr size " << optr_addr->data_size_in_segment();
    entry->reset(optr_addr, construct_args);
    size_ += entry->size;
    optr_addr->set_victim(true);
    entries_.splice(entries_.begin(), entries_, entry);
    return true;
  }

  auto entry = entries_.emplace_front(optr_addr, construct_args);
  entry.optr->set_victim(true);
  map_[optr_addr] = entries_.begin();
  cnt_++;
  size_ += entry.size;

  while (size() > size_limit_ || count() > cnt_limit_) {
    pop_back_locked();
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

  auto entry = iter->second;
  cnt_--;
  size_ -= entry->size;
  entry->optr->set_victim(false);
  map_.erase(iter->first);
  entries_.erase(entry);
  return true;
}

/** Util functions. All functions should be called with lock */
inline void VictimCache::pop_back_locked() noexcept {
  auto entry = entries_.back();
  cnt_--;
  size_ -= entry.size;
  entry.optr->set_victim(false);
  map_.erase(entry.optr);
  entries_.pop_back();
}
} // namespace midas
#pragma once

#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>

#include "object.hpp"

namespace midas {

struct VCEntry {
#pragma pack(push, 1)
  ObjectPtr *optr;
  void *construct_args;
  VCEntry *prev;
  VCEntry *next;
#pragma pack(pop)
  VCEntry();
  VCEntry(ObjectPtr *optr_, void *construct_args_);
};

static_assert(sizeof(VCEntry) <=
                  sizeof(ObjectPtr) + sizeof(void *) + sizeof(VCEntry *) * 2,
              "VCEntry is not correctly aligned!");

class VictimCache {
public:
  VictimCache(size_t size_limit = std::numeric_limits<size_t>::max(),
              size_t cnt_limit = std::numeric_limits<size_t>::max());
  ~VictimCache();

  bool push_back(ObjectPtr *optr_addr, void *construct_args);
  VCEntry *pop_front();
  bool remove(ObjectPtr *optr_addr);

  size_t size() const noexcept;
  size_t count() const noexcept;

private:
  VCEntry *remove_locked(VCEntry *entry);

  std::mutex mtx_;
  VCEntry *entries_; // The LRU list
  // optr_addr -> construct_args
  std::unordered_map<ObjectPtr *, VCEntry *> map_;

  size_t size_limit_;
  size_t cnt_limit_;
  size_t size_;
  size_t cnt_;
};

} // namespace midas

#include "impl/victim_cache.ipp"
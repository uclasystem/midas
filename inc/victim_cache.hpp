#pragma once

#include <cstdint>
#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>

#include "object.hpp"

namespace midas {

struct VCEntry {
#pragma pack(push, 1)
  ObjectPtr *optr;
  size_t size;
  void *construct_args;
  VCEntry *prev;
  VCEntry *next;
#pragma pack(pop)
  VCEntry();
  VCEntry(ObjectPtr *optr_, void *construct_args_);
};

static_assert(sizeof(VCEntry) <= sizeof(ObjectPtr) + sizeof(size_t) +
                                     sizeof(void *) + sizeof(VCEntry *) * 2,
              "VCEntry is not correctly aligned!");

class VictimCache {
public:
  VictimCache(int64_t size_limit = std::numeric_limits<int64_t>::max(),
              int64_t cnt_limit = std::numeric_limits<int64_t>::max());
  ~VictimCache();

  bool push_back(ObjectPtr *optr_addr, void *construct_args);
  VCEntry *pop_front();
  bool remove(ObjectPtr *optr_addr);

  int64_t size() const noexcept;
  int64_t count() const noexcept;

private:
  VCEntry *remove_locked(VCEntry *entry);

  std::mutex mtx_;
  VCEntry *entries_; // The LRU list
  // optr_addr -> construct_args
  std::unordered_map<ObjectPtr *, VCEntry *> map_;

  int64_t size_limit_;
  int64_t cnt_limit_;
  int64_t size_;
  int64_t cnt_;
};

} // namespace midas

#include "impl/victim_cache.ipp"
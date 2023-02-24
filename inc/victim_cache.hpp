#pragma once

#include <limits>
#include <mutex>
#include <numeric>

#include "object.hpp"

namespace cachebank {

struct VCEntry {
#pragma pack(push, 1)
  ObjectPtr optr;
  void *construct_args;
  VCEntry *prev;
  VCEntry *next;
#pragma pack(pop)
};

static_assert(sizeof(VCEntry) <=
                  sizeof(ObjectPtr) + sizeof(void *) + sizeof(VCEntry *) * 2,
              "VCEntry is not correctly aligned!");

class VictimCache {
public:
  VictimCache(size_t size_limit = std::numeric_limits<size_t>::max(),
              size_t cnt_limit = std::numeric_limits<size_t>::max());
  ~VictimCache();

  void push_back(VCEntry *entry);
  VCEntry *pop_front();
  void remove(VCEntry *entry);

  size_t size() const noexcept;
  size_t count() const noexcept;

private:
  VCEntry *remove_locked(VCEntry *entry);

  std::mutex mtx_;
  VCEntry *entries_;

  size_t size_limit_;
  size_t cnt_limit_;
  size_t size_;
  size_t cnt_;
};

} // namespace cachebank

#include "impl/victim_cache.ipp"
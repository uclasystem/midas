#pragma once

#include <cstdint>
#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>
#include <list>

#include "object.hpp"

namespace midas {
constexpr static bool kEnableVictimCache = true;

struct VCEntry {
#pragma pack(push, 1)
  ObjectPtr *optr;
  size_t size;
  void *construct_args;
#pragma pack(pop)
  VCEntry();
  VCEntry(ObjectPtr *optr_, void *construct_args_);
  void reset(ObjectPtr *optr_, void *construct_args_) noexcept;
};

static_assert(sizeof(VCEntry) <=
                  sizeof(ObjectPtr) + sizeof(size_t) + sizeof(void *),
              "VCEntry is not correctly aligned!");

class VictimCache {
public:
  VictimCache(int64_t size_limit = std::numeric_limits<int64_t>::max(),
              int64_t cnt_limit = std::numeric_limits<int64_t>::max());
  ~VictimCache();

  bool put(ObjectPtr *optr_addr, void *construct_args) noexcept;
  bool remove(ObjectPtr *optr_addr) noexcept;

  int64_t size() const noexcept;
  int64_t count() const noexcept;

private:
  using VCIter = std::list<VCEntry>::iterator;
  void pop_back_locked() noexcept;
  inline void create_entry(ObjectPtr *optr_addr, void *construct_args) noexcept;
  inline void delete_entry(VCEntry *entry) noexcept;

  std::mutex mtx_;
  std::list<VCEntry> entries_; // LRU list
  std::unordered_map<ObjectPtr *, VCIter> map_; // optr * -> construct_args

  int64_t size_limit_;
  int64_t cnt_limit_;
  int64_t size_;
  int64_t cnt_;
};

} // namespace midas

#include "impl/victim_cache.ipp"
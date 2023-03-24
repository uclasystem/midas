#pragma once

#include <cstdint>
#include <limits>
#include <mutex>
#include <numeric>
#include <unordered_map>

#include "object.hpp"

namespace midas {
constexpr static bool kEnableVictimCache = true;

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
  void reset(ObjectPtr *optr_, void *construct_args_) noexcept;
};

static_assert(sizeof(VCEntry) <= sizeof(ObjectPtr) + sizeof(size_t) +
                                     sizeof(void *) + sizeof(VCEntry *) * 2,
              "VCEntry is not correctly aligned!");

class VictimCache {
public:
  VictimCache(int64_t size_limit = std::numeric_limits<int64_t>::max(),
              int64_t cnt_limit = std::numeric_limits<int64_t>::max());
  ~VictimCache();

  bool push_back(ObjectPtr *optr_addr, void *construct_args) noexcept;
  bool remove(ObjectPtr *optr_addr) noexcept;

  int64_t size() const noexcept;
  int64_t count() const noexcept;

private:
  void pop_front_locked() noexcept;
  void remove_locked(VCEntry *entry) noexcept;
  inline VCEntry *create_entry(ObjectPtr *optr_addr,
                               void *construct_args) noexcept;
  inline void delete_entry(VCEntry *entry) noexcept;

  std::mutex mtx_;
  VCEntry *entries_;                               // The LRU list
  std::unordered_map<ObjectPtr *, VCEntry *> map_; // optr * -> construct_args

  int64_t size_limit_;
  int64_t cnt_limit_;
  int64_t size_;
  int64_t cnt_;
};

} // namespace midas

#include "impl/victim_cache.ipp"
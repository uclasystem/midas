#pragma once

#include "log.hpp"
#include "object.hpp"
#include "cache_manager.hpp"

namespace midas {

template <typename Tp, typename Alloc = LogAllocator,
          typename Lock = std::mutex>
class SyncList {
public:
  SyncList();
  SyncList(CachePool *pool);

  std::unique_ptr<Tp> pop();
  bool pop(Tp &v);
  bool push(const Tp &v);
  bool clear();

  bool empty() const noexcept;
  bool size() const noexcept;

private:
  struct ListNode {
    ObjectPtr obj;
    ListNode *next;
  };

  ListNode *create_node(const Tp &v);
  void delete_node(ListNode *node);

  CachePool *pool_;

  Lock lock_;

  ListNode *list_;
  std::atomic_int32_t size_;
};
} // namespace midas

#include "impl/sync_list.ipp"
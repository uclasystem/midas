namespace midas {
template <typename Tp, typename Alloc, typename Lock>
SyncList<Tp, Alloc, Lock>::SyncList() : size_(0), list_(nullptr) {
  pool_ = CachePool::global_cache_pool();
}

template <typename Tp, typename Alloc, typename Lock>
SyncList<Tp, Alloc, Lock>::SyncList(CachePool *pool)
    : pool_(pool), size_(0), list_(nullptr) {}

template <typename Tp, typename Alloc, typename Lock>
inline bool SyncList<Tp, Alloc, Lock>::empty() const noexcept {
  return size_ == 0;
}

template <typename Tp, typename Alloc, typename Lock>
inline bool SyncList<Tp, Alloc, Lock>::size() const noexcept {
  return size_;
}

template <typename Tp, typename Alloc, typename Lock>
inline std::unique_ptr<Tp> SyncList<Tp, Alloc, Lock>::pop() {
  Tp *stored_v = reinterpret_cast<Tp *>(::operator new(sizeof(Tp)));
  if (pop(*stored_v))
    return std::unique_ptr<Tp>(stored_v);
  return nullptr;
}

template <typename Tp, typename Alloc, typename Lock>
bool SyncList<Tp, Alloc, Lock>::pop(Tp &v) {
  lock_.lock();
  if (!list_) {
    lock_.unlock();
    pool_->inc_cache_miss();
    return false;
  }
  auto node = list_;
  list_ = list_->next;
  size_--;
  lock_.unlock();
  if (node->obj.is_victim())
    pool_->inc_cache_victim_hit();
  if (node->obj.null() || !node->obj.copy_to(&v, sizeof(Tp))) {
    delete_node(node);
    return false;
  }
  pool_->inc_cache_hit();
  LogAllocator::count_access();
  return true;
}

template <typename Tp, typename Alloc, typename Lock>
bool SyncList<Tp, Alloc, Lock>::push(const Tp &v) {
  ListNode *node = create_node(v);
  if (!node)
    return false;
  lock_.lock();
  node->next = list_;
  list_ = node;
  size_++;
  lock_.unlock();
  LogAllocator::count_access();
  return true;
}

template <typename Tp, typename Alloc, typename Lock>
bool SyncList<Tp, Alloc, Lock>::clear() {
  lock_.lock();
  while (list_) {
    auto node = list_;
    list_ = list_->next;
    delete_node(node);
    size_--;
  }
  lock_.unlock();
  return true;
}

template <typename Tp, typename Alloc, typename Lock>
typename SyncList<Tp, Alloc, Lock>::ListNode *
SyncList<Tp, Alloc, Lock>::create_node(const Tp &v) {
  auto allocator = pool_->get_allocator();
  ListNode *node = new ListNode();

  if (!allocator->alloc_to(sizeof(Tp), &node->obj) ||
      !node->obj.copy_from(&v, sizeof(Tp))) {
    delete node;
    return nullptr;
  }
  // assert(!node->obj.null());
  if (node->obj.null()) {
    LOG(kError) << "new list node is freed!";
    delete node;
    return nullptr;
  }
  node->next = nullptr;
  return node;
}

template <typename Tp, typename Alloc, typename Lock>
void SyncList<Tp, Alloc, Lock>::delete_node(ListNode *node) {
  // node->obj.free();
  pool_->free(node->obj);
  delete node;
}

} // namespace midas
#pragma once

#include <functional>
#include <memory>

#include "object.hpp"
#include "transient_ptr.hpp"
#include <optional>

namespace midas {

template <typename T, typename... ReconArgs> class SoftMemPool;

template <typename T, typename... ReconArgs> class GenericSoftPtr {
public:
  using ReconFunc = std::function<T(ReconArgs...)>;
};

template <typename T, typename... ReconArgs>
class SoftUniquePtr : public GenericSoftPtr<T, ReconArgs...> {
public:
  using SoftMemoryPool = SoftMemPool<T, ReconArgs...>;
  ~SoftUniquePtr();

  // Disable copy constructor and copy assignment.
  SoftUniquePtr(const SoftUniquePtr &) = delete;
  SoftUniquePtr &operator=(const SoftUniquePtr &) = delete;
  // Enable move constructor and move assignment.
  SoftUniquePtr(SoftUniquePtr &&) noexcept;
  SoftUniquePtr &operator=(SoftUniquePtr &&) noexcept;

  // Access
  T read(ReconArgs... args);
  void write(const T &);
  bool cmpxchg(const T &oldval, const T &newval);

  void reset();

private:
  SoftUniquePtr(SoftMemoryPool *pool) noexcept;
  friend SoftMemoryPool;

  ObjectPtr ptr_;
  SoftMemoryPool *pool_;
};

} // namespace midas

#include "impl/soft_unique_ptr.ipp"
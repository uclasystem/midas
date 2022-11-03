#include <memory>
#include <mutex>

#include "../inc/utils.hpp"

namespace cachebank {
static inline bool in_volatile_range(uint64_t addr) {
  return addr >= cachebank::kVolatileSttAddr &&
         addr < cachebank::kVolatileEndAddr;
}

inline SigHandler *SigHandler::global_sighandler() {
  static std::mutex mtx_;
  static std::shared_ptr<SigHandler> hdler_;
  if (hdler_)
    return hdler_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (hdler_)
    return hdler_.get();
  hdler_ = std::make_shared<SigHandler>();
  return hdler_.get();
}

} // namespace cachebank
#include <memory>
#include <mutex>

namespace midas {
static inline bool in_volatile_range(uint64_t addr) {
  return addr >= midas::kVolatileSttAddr &&
         addr < midas::kVolatileEndAddr;
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

} // namespace midas
#pragma once

namespace cachebank {

/** Generic Object */
inline bool GenericObjecthdr::is_valid() const noexcept {
  return (flags & (1 << kPresentBit));
}

inline bool GenericObjecthdr::is_small_obj() const noexcept {
  return (flags & (1 << kSmallObjBit));
}

/** Small Object */
inline void SmallObjectHdr::init(uint32_t size_) noexcept {
  set_size(size_);
  set_present();
  _small_obj();
}

inline void SmallObjectHdr::free() noexcept {
  clr_present();
}

inline void SmallObjectHdr::set_size(uint32_t size_) noexcept { size = size_; }
inline uint16_t SmallObjectHdr::get_size() const noexcept { return size; }

inline void SmallObjectHdr::set_rref(uint64_t addr) noexcept { rref = addr; }
inline uint64_t SmallObjectHdr::get_rref() const noexcept { return rref; }

inline void SmallObjectHdr::set_present() noexcept {
  flags |= (1 << kPresentBit);
}
inline void SmallObjectHdr::clr_present() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void SmallObjectHdr::set_accessed() noexcept {
  flags |= (1 << kPresentBit);
}
inline void SmallObjectHdr::clr_accessed() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void SmallObjectHdr::set_evacuate() noexcept {
  flags |= (1 << kPresentBit);
}
inline void SmallObjectHdr::clr_evacuate() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void SmallObjectHdr::_small_obj() noexcept {
  flags |= 1 << kSmallObjBit;
}

inline void SmallObjectHdr::set_invalid() noexcept {
  auto *meta = reinterpret_cast<uint64_t *>(this);
  *meta = kInvalidHdr;
}

/** Large Object */
inline void LargeObjectHdr::init(uint32_t size_) noexcept {
  set_size(size_);
  set_present();
  _large_obj();
}

inline void LargeObjectHdr::free() noexcept {
  clr_present();
}

inline void LargeObjectHdr::set_size(uint32_t size_) noexcept { size = size_; }
inline uint32_t LargeObjectHdr::get_size() const noexcept { return size; }

inline void LargeObjectHdr::set_present() noexcept {
  flags |= (1 << kPresentBit);
}
inline void LargeObjectHdr::clr_present() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void LargeObjectHdr::set_accessed() noexcept {
  flags |= (1 << kPresentBit);
}
inline void LargeObjectHdr::clr_accessed() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void LargeObjectHdr::set_evacuate() noexcept {
  flags |= (1 << kPresentBit);
}
inline void LargeObjectHdr::clr_evacuate() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline void LargeObjectHdr::_large_obj() noexcept {
  flags &= ~(1 << kSmallObjBit);
}

} // namespace cachebank
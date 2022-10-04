#pragma once

#include <cassert>

#include "logging.hpp"
#include "utils.hpp"

namespace cachebank {

/** Generic Object */
inline void GenericObjectHdr::set_invalid() noexcept { flags = kInvalidHdr; }

inline bool GenericObjectHdr::is_valid() const noexcept {
  return flags != kInvalidHdr;
}

inline bool GenericObjectHdr::is_small_obj() const noexcept {
  return (flags & (1ull << kSmallObjBit));
}

/** Small Object */
inline void SmallObjectHdr::init(uint32_t size_, uint64_t rref) noexcept {
  set_size(size_);
  set_rref(rref);
  set_present();
  _small_obj();
}

inline void SmallObjectHdr::free() noexcept { clr_present(); }

inline void SmallObjectHdr::set_invalid() noexcept {
  auto *meta = reinterpret_cast<uint64_t *>(this);
  *meta = kInvalidHdr;
}

inline bool SmallObjectHdr::is_valid() noexcept {
  auto *meta = reinterpret_cast<uint64_t *>(this);
  return *meta != kInvalidHdr;
}

inline void SmallObjectHdr::set_size(uint32_t size_) noexcept {
  size_ = round_up_to_align(size_, kSmallObjSizeUnit);
  size_ /= kSmallObjSizeUnit;
  assert(size_ < (1 << 12));
  size = size_;
}
inline uint32_t SmallObjectHdr::get_size() const noexcept {
  return size * kSmallObjSizeUnit;
}

inline void SmallObjectHdr::set_rref(uint64_t addr) noexcept { rref = addr; }
inline uint64_t SmallObjectHdr::get_rref() const noexcept { return rref; }

inline bool SmallObjectHdr::is_present() const noexcept {
  return flags & (1 << kPresentBit);
}
inline void SmallObjectHdr::set_present() noexcept {
  flags |= (1 << kPresentBit);
}
inline void SmallObjectHdr::clr_present() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline bool SmallObjectHdr::is_accessed() const noexcept {
  return flags & (1 << kAccessedBit);
}
inline void SmallObjectHdr::set_accessed() noexcept {
  flags |= (1 << kAccessedBit);
}
inline void SmallObjectHdr::clr_accessed() noexcept {
  flags &= ~(1 << kAccessedBit);
}

inline bool SmallObjectHdr::is_evacuate() const noexcept {
  return flags & (1 << kEvacuateBit);
}
inline void SmallObjectHdr::set_evacuate() noexcept {
  flags |= (1 << kEvacuateBit);
}
inline void SmallObjectHdr::clr_evacuate() noexcept {
  flags &= ~(1 << kEvacuateBit);
}

inline void SmallObjectHdr::_small_obj() noexcept {
  flags |= 1 << kSmallObjBit;
}

/** Large Object */
inline void LargeObjectHdr::init(uint32_t size_, uint64_t rref) noexcept {
  set_size(size_);
  set_rref(rref);
  set_present();
  _large_obj();
}

inline void LargeObjectHdr::set_invalid() noexcept {
  auto *meta = reinterpret_cast<uint64_t *>(this);
  *meta = kInvalidHdr;
}

inline bool LargeObjectHdr::is_valid() noexcept {
  auto *meta = reinterpret_cast<uint64_t *>(this);
  return *meta != kInvalidHdr;
}

inline void LargeObjectHdr::free() noexcept { clr_present(); }

inline void LargeObjectHdr::set_size(uint32_t size_) noexcept { size = size_; }
inline uint32_t LargeObjectHdr::get_size() const noexcept { return size; }

inline void LargeObjectHdr::set_rref(uint64_t addr) noexcept { rref = addr; }
inline uint64_t LargeObjectHdr::get_rref() const noexcept { return rref; }

inline bool LargeObjectHdr::is_present() const noexcept {
  return flags & (1 << kPresentBit);
}
inline void LargeObjectHdr::set_present() noexcept {
  flags |= (1 << kPresentBit);
}
inline void LargeObjectHdr::clr_present() noexcept {
  flags &= ~(1 << kPresentBit);
}

inline bool LargeObjectHdr::is_accessed() const noexcept {
  return flags & (1 << kAccessedBit);
}
inline void LargeObjectHdr::set_accessed() noexcept {
  flags |= (1 << kAccessedBit);
}
inline void LargeObjectHdr::clr_accessed() noexcept {
  flags &= ~(1 << kAccessedBit);
}

inline bool LargeObjectHdr::is_evacuate() const noexcept {
  return flags & (1 << kEvacuateBit);
}
inline void LargeObjectHdr::set_evacuate() noexcept {
  flags |= (1 << kEvacuateBit);
}
inline void LargeObjectHdr::clr_evacuate() noexcept {
  flags &= ~(1 << kEvacuateBit);
}

inline bool LargeObjectHdr::is_continue() const noexcept {
  return flags & (1 << kContinueBit);
}
inline void LargeObjectHdr::set_continue() noexcept {
  flags |= (1 << kContinueBit);
}
inline void LargeObjectHdr::clr_continue() noexcept {
  flags &= ~(1 << kContinueBit);
}

inline void LargeObjectHdr::_large_obj() noexcept {
  flags &= ~(1 << kSmallObjBit);
}

/** ObjectPtr */
inline bool ObjectPtr::set(uint64_t stt_addr, size_t data_size) {
  size_ = round_up_to_align(data_size, kSmallObjSizeUnit);

  auto obj_size = total_size();
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    hdr.init(data_size);
    obj_ = TransientPtr(stt_addr, obj_size);
    return obj_.copy_from(&hdr, sizeof(hdr));
  } else { // large obj
    LargeObjectHdr hdr;
    hdr.init(data_size);
    obj_ = TransientPtr(stt_addr, obj_size);
    return obj_.copy_from(&hdr, sizeof(hdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::init_from_soft(uint64_t soft_addr) {
  GenericObjectHdr hdr;
  auto tptr = TransientPtr(soft_addr, sizeof(GenericObjectHdr));
  if (!tptr.copy_to(&hdr, sizeof(hdr)))
    return false;

  if (hdr.is_small_obj()) {
    SmallObjectHdr shdr = *(reinterpret_cast<SmallObjectHdr *>(&hdr));
    size_ = shdr.get_size();
    obj_ = TransientPtr(soft_addr, total_size());
  } else {
    LargeObjectHdr lhdr;
    if (!tptr.copy_to(&lhdr, sizeof(lhdr)))
      return false;
    size_ = lhdr.get_size();
    obj_ = TransientPtr(soft_addr, total_size());
  }

  return true;
}

inline bool ObjectPtr::free() noexcept {
  if (!is_valid())
    return false;
  return clr_present();
}

inline size_t ObjectPtr::total_size(size_t data_size) noexcept {
  data_size = round_up_to_align(data_size, kSmallObjSizeUnit);
  return data_size < kSmallObjThreshold ? sizeof(SmallObjectHdr) + data_size
                                        : sizeof(LargeObjectHdr) + data_size;
}

inline size_t ObjectPtr::total_size() const noexcept {
  return hdr_size() + data_size();
}
inline size_t ObjectPtr::hdr_size() const noexcept {
  return is_small_obj() ? sizeof(SmallObjectHdr) : sizeof(LargeObjectHdr);
}
inline size_t ObjectPtr::data_size() const noexcept { return size_; }

inline bool ObjectPtr::is_small_obj() const noexcept {
  return size_ < kSmallObjThreshold;
}

inline bool ObjectPtr::set_rref(uint64_t addr) noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_rref(addr);
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_rref(addr);
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline uint64_t ObjectPtr::get_rref() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    return hdr.get_rref();
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    return hdr.get_rref();
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::is_valid() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    return hdr.is_valid();
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    return hdr.is_valid();
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::set_invalid() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_invalid();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_invalid();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::is_present() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    return hdr.is_present();
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    return hdr.is_present();
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::set_present() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_present();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LOG(kError) << "large obj allocation is not implemented yet!";
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_present();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::clr_present() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.clr_present();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LOG(kError) << "large obj allocation is not implemented yet!";
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.clr_present();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::is_accessed() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    return hdr.is_accessed();
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    return hdr.is_accessed();
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::set_accessed() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_accessed();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_accessed();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::clr_accessed() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.clr_accessed();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.clr_accessed();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
  LOG(kError) << "impossible to reach here!";
  return false;
}

inline bool ObjectPtr::is_evacuate() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.is_evacuate();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.is_evacuate();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
}

inline bool ObjectPtr::set_evacuate() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_evacuate();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_evacuate();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
}

inline bool ObjectPtr::clr_evacuate() noexcept {
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.clr_evacuate();
    return obj_.copy_from(&hdr, sizeof(SmallObjectHdr));
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.clr_evacuate();
    return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
  }
}

inline bool ObjectPtr::is_continue() noexcept {
  assert(!is_small_obj());
  LargeObjectHdr hdr;
  if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
    return false;
  return hdr.is_continue();
}

inline bool ObjectPtr::set_continue() noexcept {
  assert(!is_small_obj());
  LargeObjectHdr hdr;
  if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
    return false;
  hdr.set_continue();
  return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
}

inline bool ObjectPtr::clr_continue() noexcept {
  assert(!is_small_obj());

  LargeObjectHdr hdr;
  if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
    return false;
  hdr.clr_continue();
  return obj_.copy_from(&hdr, sizeof(LargeObjectHdr));
}

inline bool ObjectPtr::cmpxchg(int64_t offset, uint64_t oldval,
                               uint64_t newval) {
  return obj_.cmpxchg(hdr_size() + offset, oldval, newval);
}

inline bool ObjectPtr::copy_from(const void *src, size_t len, int64_t offset) {
  if (!set_accessed())
    return false;
  return obj_.copy_from(src, len, hdr_size() + offset);
}

inline bool ObjectPtr::copy_to(void *dst, size_t len, int64_t offset) {
  if (!set_accessed())
    return false;
  return obj_.copy_to(dst, len, hdr_size() + offset);
}

inline bool ObjectPtr::copy_from(ObjectPtr &src, size_t len,
                                 int64_t from_offset, int64_t to_offset) {
  if (!src.set_accessed() || !set_accessed())
    return false;
  return obj_.copy_from(src.obj_, len, src.hdr_size() + from_offset,
                        hdr_size() + to_offset);
}

inline bool ObjectPtr::copy_to(ObjectPtr &dst, size_t len, int64_t from_offset,
                               int64_t to_offset) {
  if (!set_accessed() || !dst.set_accessed())
    return false;
  return obj_.copy_to(dst.obj_, len, hdr_size() + from_offset,
                      dst.hdr_size() + to_offset);
}

} // namespace cachebank
#pragma once

#include <cassert>

#include "logging.hpp"
#include "utils.hpp"

namespace cachebank {

/** Generic Object */
inline MetaObjectHdr::MetaObjectHdr() : flags(0) {}

inline void MetaObjectHdr::set_invalid() noexcept { flags = kInvalidHdr; }

inline bool MetaObjectHdr::is_valid() const noexcept {
  return flags != kInvalidHdr;
}

inline bool MetaObjectHdr::is_small_obj() const noexcept {
  return (flags & (1ull << kSmallObjBit));
}
inline void MetaObjectHdr::set_small_obj() noexcept {
  flags |= (1ull << kSmallObjBit);
}

inline void MetaObjectHdr::set_large_obj() noexcept {
  flags &= ~(1ull << kSmallObjBit);
}

inline bool MetaObjectHdr::is_present() const noexcept {
  return flags & (1ull << kPresentBit);
}
inline void MetaObjectHdr::set_present() noexcept {
  flags |= (1ull << kPresentBit);
}
inline void MetaObjectHdr::clr_present() noexcept {
  flags &= ~(1ull << kPresentBit);
}

inline bool MetaObjectHdr::is_accessed() const noexcept {
  return flags & (1ull << kAccessedBit);
}
inline void MetaObjectHdr::set_accessed() noexcept {
  flags |= (1ull << kAccessedBit);
}
inline void MetaObjectHdr::clr_accessed() noexcept {
  flags &= ~(1ull << kAccessedBit);
}

inline bool MetaObjectHdr::is_evacuate() const noexcept {
  return flags & (1ull << kEvacuateBit);
}
inline void MetaObjectHdr::set_evacuate() noexcept {
  flags |= (1ull << kEvacuateBit);
}
inline void MetaObjectHdr::clr_evacuate() noexcept {
  flags &= ~(1ull << kEvacuateBit);
}

inline bool MetaObjectHdr::is_continue() const noexcept {
  return flags & (1ull << kContinueBit);
}
inline void MetaObjectHdr::set_continue() noexcept {
  flags |= (1ull << kContinueBit);
}
inline void MetaObjectHdr::clr_continue() noexcept {
  flags &= ~(1ull << kContinueBit);
}

inline MetaObjectHdr *MetaObjectHdr::cast_from(void *hdr) noexcept {
  return reinterpret_cast<MetaObjectHdr *>(hdr);
}

/** Small Object */
inline SmallObjectHdr::SmallObjectHdr() : rref(0), size(0), flags(0) {}

inline void SmallObjectHdr::init(uint32_t size, uint64_t rref) noexcept {
  auto meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  meta_hdr->set_present();
  meta_hdr->set_small_obj();

  set_size(size);
  set_rref(rref);
}

inline void SmallObjectHdr::set_invalid() noexcept {
  auto *meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  meta_hdr->set_invalid();

  auto bytes = *(reinterpret_cast<uint64_t *>(this));
  assert(bytes == kInvalidHdr);
}

inline bool SmallObjectHdr::is_valid() noexcept {
  auto *meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  return meta_hdr->is_valid();
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

inline void SmallObjectHdr::set_flags(uint8_t flags_) noexcept {
  flags = flags_;
}
inline uint8_t SmallObjectHdr::get_flags() const noexcept { return flags; }

/** Large Object */
inline LargeObjectHdr::LargeObjectHdr() : size(0), flags(0), rref(0), next(0) {}

inline void LargeObjectHdr::init(uint32_t size_, uint64_t rref) noexcept {
  auto *meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  meta_hdr->set_present();
  meta_hdr->set_large_obj();
  meta_hdr->clr_continue();

  set_size(size_);
  set_rref(rref);
}

inline void LargeObjectHdr::set_invalid() noexcept {
  auto *meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  meta_hdr->set_invalid();
}

inline bool LargeObjectHdr::is_valid() noexcept {
  auto *meta_hdr = reinterpret_cast<MetaObjectHdr *>(this);
  return meta_hdr->is_valid();
}

inline void LargeObjectHdr::set_size(uint32_t size_) noexcept { size = size_; }
inline uint32_t LargeObjectHdr::get_size() const noexcept { return size; }

inline void LargeObjectHdr::set_rref(uint64_t addr) noexcept { rref = addr; }
inline uint64_t LargeObjectHdr::get_rref() const noexcept { return rref; }

inline void LargeObjectHdr::set_next(uint64_t addr) noexcept { next = addr; }
inline uint64_t LargeObjectHdr::get_next() const noexcept { return next; }

inline void LargeObjectHdr::set_flags(uint32_t flags_) noexcept {
  flags = flags_;
}
inline uint32_t LargeObjectHdr::get_flags() const noexcept { return flags; }

/** ObjectPtr */
inline ObjectPtr::ObjectPtr() : size_(0), obj_() {}

inline bool ObjectPtr::null() const noexcept { return obj_.null(); }

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

using RetCode = ObjectPtr::RetCode;
inline RetCode ObjectPtr::set(uint64_t stt_addr, size_t data_size) {
  size_ = round_up_to_align(data_size, kSmallObjSizeUnit);

  auto obj_size = total_size();
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    hdr.init(data_size);
    obj_ = TransientPtr(stt_addr, obj_size);
    return obj_.copy_from(&hdr, sizeof(hdr)) ? RetCode::Succ : RetCode::Fault;
  } else { // large obj
    LargeObjectHdr hdr;
    hdr.init(data_size);
    obj_ = TransientPtr(stt_addr, obj_size);
    return obj_.copy_from(&hdr, sizeof(hdr)) ? RetCode::Succ : RetCode::Fault;
  }
  LOG(kError) << "impossible to reach here!";
  return RetCode::Fail;
}

inline RetCode ObjectPtr::init_from_soft(uint64_t soft_addr) {
  MetaObjectHdr hdr;
  obj_ = TransientPtr(soft_addr, sizeof(MetaObjectHdr));
  if (!obj_.copy_to(&hdr, sizeof(hdr)))
    return RetCode::Fault;

  if (!hdr.is_valid())
    return RetCode::Fail;

  if (hdr.is_small_obj()) {
    SmallObjectHdr shdr = *(reinterpret_cast<SmallObjectHdr *>(&hdr));
    size_ = shdr.get_size();
    obj_ = TransientPtr(soft_addr, total_size());
  } else {
    LargeObjectHdr lhdr;
    if (!obj_.copy_to(&lhdr, sizeof(lhdr)))
      return RetCode::Fault;
    size_ = lhdr.get_size();
    obj_ = TransientPtr(soft_addr, total_size());
  }

  return RetCode::Succ;
}

inline RetCode ObjectPtr::free_small() noexcept {
  assert(!null());

  auto opt_meta = get_meta_hdr();
  if (!opt_meta)
    return RetCode::Fault;

  auto meta_hdr = *opt_meta;
  if (!meta_hdr.is_valid())
    return RetCode::Fail;
  meta_hdr.clr_present();
  auto ret = set_meta_hdr(meta_hdr);
  auto rref = reinterpret_cast<ObjectPtr *>(get_rref());
  if (rref)
    rref->obj_.reset();
  return ret;
}

inline RetCode ObjectPtr::free_large() noexcept {
  assert(!null());

  auto opt_meta = get_meta_hdr();
  if (!opt_meta)
    return RetCode::Fault;
  LargeObjectHdr hdr;
  if (!obj_.copy_to(&hdr, sizeof(hdr)))
    return RetCode::Fault;

  auto meta_hdr = *opt_meta;
  if (!meta_hdr.is_valid())
    return RetCode::Fail;
  meta_hdr.clr_present();
  auto ret = set_meta_hdr(meta_hdr);

  auto rref = reinterpret_cast<ObjectPtr *>(get_rref());
  if (rref)
    rref->obj_.reset();

  auto next = hdr.get_next();
  while (next) {
    ObjectPtr optr;
    if (optr.init_from_soft(next) != RetCode::Succ ||
        !optr.obj_.copy_to(&hdr, sizeof(hdr))) {
      return RetCode::Fault;
    }
    next = hdr.get_next();

    auto meta_hdr = MetaObjectHdr::cast_from(this);
    meta_hdr->clr_present();
    if (!optr.obj_.copy_from(&hdr, sizeof(hdr))) {
      return RetCode::Fault;
    }
  }
  return RetCode::Succ;
}

inline bool ObjectPtr::set_rref(uint64_t addr) noexcept {
  assert(!null());
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return false;
    hdr.set_rref(addr);
    if (!obj_.copy_from(&hdr, sizeof(SmallObjectHdr)))
      return false;
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return false;
    hdr.set_rref(addr);
    if (!obj_.copy_from(&hdr, sizeof(LargeObjectHdr)))
      return false;
  }
  return true;
}

inline bool ObjectPtr::set_rref(ObjectPtr *addr) noexcept {
  return set_rref(reinterpret_cast<uint64_t>(addr));
}

inline ObjectPtr *ObjectPtr::get_rref() noexcept {
  assert(!null());
  if (is_small_obj()) {
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(SmallObjectHdr)))
      return nullptr;
    return reinterpret_cast<ObjectPtr *>(hdr.get_rref());
  } else {
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(LargeObjectHdr)))
      return nullptr;
    return reinterpret_cast<ObjectPtr *>(hdr.get_rref());
  }
  LOG(kError) << "impossible to reach here!";
  return nullptr;
}

inline RetCode ObjectPtr::upd_rref() noexcept {
  auto *ref = reinterpret_cast<ObjectPtr *>(get_rref());
  if (!ref)
    return RetCode::Fail;
  *ref = *this;
  return RetCode::Succ;
}

inline std::optional<MetaObjectHdr> ObjectPtr::get_meta_hdr() noexcept {
  MetaObjectHdr meta_hdr;
  if (!obj_.copy_to(&meta_hdr, sizeof(meta_hdr)))
    return std::nullopt;
  return meta_hdr;
}

inline RetCode ObjectPtr::set_meta_hdr(const MetaObjectHdr &meta_hdr) noexcept {
  if (is_small_obj()) {
    auto flags =
        reinterpret_cast<const SmallObjectHdr *>(&meta_hdr)->get_flags();
    SmallObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(hdr)))
      return RetCode::Fault;
    hdr.set_flags(flags);
    if (!obj_.copy_from(&hdr, sizeof(hdr)))
      return RetCode::Fault;
  } else {
    auto flags =
        reinterpret_cast<const LargeObjectHdr *>(&meta_hdr)->get_flags();
    LargeObjectHdr hdr;
    if (!obj_.copy_to(&hdr, sizeof(hdr)))
      return RetCode::Fault;
    hdr.set_flags(flags);
    if (!obj_.copy_from(&hdr, sizeof(hdr)))
      return RetCode::Fault;
  }
  return RetCode::Succ;
}

inline bool ObjectPtr::cmpxchg(int64_t offset, uint64_t oldval,
                               uint64_t newval) {
  if (null())
    return false;
  return obj_.cmpxchg(hdr_size() + offset, oldval, newval);
}

inline bool ObjectPtr::copy_from(const void *src, size_t len, int64_t offset) {
  return is_small_obj() ? copy_from_small(src, len, offset)
                        : copy_from_large(src, len, offset);
}

inline bool ObjectPtr::copy_to(void *dst, size_t len, int64_t offset) {
  return is_small_obj() ? copy_to_small(dst, len, offset)
                        : copy_to_large(dst, len, offset);
}

inline RetCode ObjectPtr::move_from(ObjectPtr &src) {
  auto ret = RetCode::Fail;
  if (null() || src.null())
    return RetCode::Fail;
  assert(src.total_size() == this->total_size());
  /* NOTE (YIFAN): the order of operations below are tricky:
   *      1. copy data from src to this.
   *      2. free src (rref will be reset to nullptr).
   *      3. mark this as present, finish setup.
   *      4. update rref, let it point to this.
   */
  if (!obj_.copy_from(src.obj_, src.total_size()))
    return RetCode::Fail;
  ret = src.free(/* locked = */ true);
  if (ret != RetCode::Succ)
    return ret;
  auto opt_meta = get_meta_hdr();
  if (!opt_meta)
    return ret;
  auto meta_hdr = *opt_meta;
  meta_hdr.set_present();
  ret = set_meta_hdr(meta_hdr);
  if (ret != RetCode::Succ)
    return ret;
  ret = upd_rref();
  if (ret != RetCode::Succ)
    return ret;
  return RetCode::Succ;
}

} // namespace cachebank
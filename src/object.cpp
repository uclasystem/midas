#include "object.hpp"
#include "obj_locker.hpp"

namespace cachebank {
LockID ObjectPtr::lock() {
  auto locker = ObjLocker::global_objlocker();
  return locker->lock(obj_);
}

void ObjectPtr::unlock(LockID id) {
  auto locker = ObjLocker::global_objlocker();
  locker->unlock(id);
}

RetCode ObjectPtr::free(bool locked) noexcept {
  if (locked)
    return is_small_obj() ? free_small() : free_large();

  auto ret = RetCode::Fail;
  LockID lock_id = lock();
  if (lock_id == -1) // lock failed as obj_ has just been reset.
    return RetCode::Fail;
  if (!null())
    ret = is_small_obj() ? free_small() : free_large();
  unlock(lock_id);
  return ret;
}

bool ObjectPtr::copy_from_small(const void *src, size_t len, int64_t offset) {
  auto ret = false;
  if (null())
    return false;
  auto lock_id = lock();
  if (lock_id == -1) // lock failed as obj_ has just been reset.
    return false;
  if (!null()) {
    auto opt_meta = load_hdr<MetaObjectHdr>(*this);
    if (!opt_meta) {
      LOG(kError);
      goto done;
    }
    auto meta_hdr = *opt_meta;
    if (!meta_hdr.is_present()) {
      LOG(kError);
      goto done;
    }
    meta_hdr.set_accessed();
    if (!store_hdr<>(meta_hdr, *this))
      goto done;

    ret = obj_.copy_from(src, len, hdr_size() + offset);
  }
done:
  unlock(lock_id);
  return ret;
}

bool ObjectPtr::copy_to_small(void *dst, size_t len, int64_t offset) {
  auto ret = false;
  if (null())
    return false;
  auto lock_id = lock();
  if (lock_id == -1) // lock failed as obj_ has just been reset.
    return false;
  if (!null()) {
    auto opt_meta = load_hdr<MetaObjectHdr>(*this);
    if (!opt_meta)
      goto done;
    auto meta_hdr = *opt_meta;
    if (!meta_hdr.is_present())
      goto done;
    meta_hdr.set_accessed();
    if (!store_hdr<>(meta_hdr, *this))
      goto done;

    ret = obj_.copy_to(dst, len, hdr_size() + offset);
  }
done:
  unlock(lock_id);
  return ret;
}

bool ObjectPtr::copy_from_large(const void *src, size_t len,
                                       int64_t offset) {
  constexpr static size_t kLogChunkAvailSize =
      kLogChunkSize - sizeof(LargeObjectHdr);

  const size_t stt_chunk = offset / kLogChunkAvailSize;
  const size_t end_chunk = (offset + len - 1) / kLogChunkAvailSize;
  int i = 0;

  auto ret = false;
  if (null())
    return false;
  auto lock_id = lock();
  if (lock_id == -1)
    return false;
  if (!null()) {
    auto opt_meta = load_hdr<MetaObjectHdr>(*this);
    if (!opt_meta)
      goto done;
    auto meta_hdr = *opt_meta;
    if (meta_hdr.is_continue() || !meta_hdr.is_present())
      goto done;
    meta_hdr.set_accessed();

    LargeObjectHdr hdr;
    ObjectPtr optr = *this;
    while (i < stt_chunk) {
      if (optr.null())
        goto done;
      if (!optr.obj_.copy_to(&hdr, sizeof(hdr)))
        goto done;
      auto next = hdr.get_next();
      if (optr.init_from_soft(next) != RetCode::Succ)
        goto done;
      i++;
    }
    auto remaining_len = len;
    int64_t offset_in_chunk = offset - stt_chunk * kLogChunkAvailSize;
    while (i <= end_chunk) {
      const auto copy_len = std::min<>(remaining_len, kLogChunkAvailSize);
      if (!optr.obj_.copy_from(src, copy_len,
                                sizeof(LargeObjectHdr) + offset_in_chunk))
        goto done;
      remaining_len -= copy_len;
      src = reinterpret_cast<const void *>(reinterpret_cast<uint64_t>(src) +
                                           copy_len);
      offset_in_chunk = 0; // start from the beginning for all followed chunks

      if (!optr.obj_.copy_to(&hdr, sizeof(hdr)))
        goto done;
      auto next = hdr.get_next();
      if (next && optr.init_from_soft(next) != RetCode::Succ)
        goto done;

      i++;
    }
    ret = true;
  }

done:
  unlock(lock_id);
  return ret;
}

bool ObjectPtr::copy_to_large(void *dst, size_t len, int64_t offset) {
  constexpr static size_t kLogChunkAvailSize =
      kLogChunkSize - sizeof(LargeObjectHdr);

  const size_t stt_chunk = offset / kLogChunkAvailSize;
  const size_t end_chunk = (offset + len - 1) / kLogChunkAvailSize;
  int i = 0;

  auto ret = false;
  if (null())
    return false;
  auto lock_id = lock();
  if (lock_id == -1)
    return false;
  if (!null()) {
    auto opt_meta = load_hdr<MetaObjectHdr>(*this);
    if (!opt_meta)
      goto done;
    auto meta_hdr = *opt_meta;
    if (meta_hdr.is_continue() || !meta_hdr.is_present())
      goto done;
    meta_hdr.set_accessed();

    LargeObjectHdr hdr;
    ObjectPtr optr = *this;
    while (i < stt_chunk) {
      if (!optr.obj_.copy_to(&hdr, sizeof(hdr)))
        goto done;
      auto next = hdr.get_next();
      if (optr.init_from_soft(next) != RetCode::Succ)
        goto done;
      i++;
    }
    auto remaining_len = len;
    auto offset_in_chunk = offset - i * kLogChunkAvailSize;
    while (i <= end_chunk) {
      const auto copy_len = std::min<>(remaining_len, kLogChunkAvailSize);
      if (!optr.obj_.copy_to(dst, copy_len,
                              sizeof(LargeObjectHdr) + offset_in_chunk))
        goto done;
      remaining_len -= copy_len;
      dst =
          reinterpret_cast<void *>(reinterpret_cast<uint64_t>(dst) + copy_len);
      offset_in_chunk = 0; // start from the beginning for all followed chunks

      if (!optr.obj_.copy_to(&hdr, sizeof(hdr)))
        goto done;
      auto next = hdr.get_next();
      if (next && optr.init_from_soft(next) != RetCode::Succ)
        goto done;

      i++;
    }
    ret = true;
  }

done:
  unlock(lock_id);
  return ret;
}

} // namespace cachebank
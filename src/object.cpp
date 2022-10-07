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

} // namespace cachebank
#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>

#include "utils.hpp"

namespace cachebank {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;
using MappedRegion = boost::interprocess::mapped_region;

class Region {
public:
  Region(uint64_t pid, uint64_t region_id) noexcept;

  friend bool operator<(const Region &lhs, const Region &rhs) noexcept {
    return lhs._region_id < rhs._region_id;
  }

  inline void *Addr() const noexcept { return _shm_region->get_address(); }
  inline uint64_t ID() const noexcept { return _region_id; }
  inline int64_t Size() const noexcept { return _size; }
  inline int64_t GetAllocBytes() const noexcept { return _alloc_bytes; }

private:
  uint64_t _pid;
  uint64_t _region_id;
  std::shared_ptr<SharedMemObj> _shm_obj;
  std::shared_ptr<MappedRegion> _shm_region;
  int64_t _size; // int64_t to adapt to boost::interprocess::offset_t
  int64_t _alloc_bytes;
};

class ResourceManager {
public:
  ResourceManager(const std::string &daemon_name = kNameCtrlQ) noexcept;
  ~ResourceManager() noexcept;

  int64_t AllocRegion(size_t size = kPageChunkSize) noexcept;
  void FreeRegions(size_t size = kPageChunkSize) noexcept;
  inline VRange GetRegion(int64_t region_id) noexcept;

  static inline ResourceManager *global_manager() noexcept;

private:
  int connect(const std::string &daemon_name = kNameCtrlQ) noexcept;
  int disconnect() noexcept;
  size_t free_region(int64_t region_id) noexcept;

  uint64_t _id;
  std::mutex _mtx;

  std::shared_ptr<MsgQueue> _ctrlq;
  std::shared_ptr<MsgQueue> _sendq;
  std::shared_ptr<MsgQueue> _recvq;

  std::map<int64_t, std::shared_ptr<Region>> _region_map;
};

#include "impl/resource_manager.ipp"
} // namespace cachebank
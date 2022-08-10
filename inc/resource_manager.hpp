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

static inline uint64_t get_unique_id() {
  auto pid = boost::interprocess::ipcdetail::get_current_process_id();
  auto creation_time =
      boost::interprocess::ipcdetail::get_current_process_creation_time();

  // TODO: unique id should be the hash of pid and creation_time to avoid pid
  // collision.
  return static_cast<uint64_t>(pid);
}

class Region {
public:
  Region(uint64_t pid, uint64_t region_id) noexcept
      : _pid(pid), _region_id(region_id), _alloc_bytes(0) {
    const auto rwmode = boost::interprocess::read_write;
    const std::string _shm_name = utils::get_region_name(_pid, _region_id);
    _shm_obj = std::make_shared<SharedMemObj>(boost::interprocess::open_only,
                                              _shm_name.c_str(), rwmode);
    _shm_obj->get_size(_size);
    void *addr = reinterpret_cast<void *>(kVolatileSttAddr +
                                          _region_id * kPageChunkSize);
    _shm_region =
        std::make_shared<MappedRegion>(*_shm_obj, rwmode, 0, _size, addr);
  }

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
  ResourceManager(const std::string &daemon_name = kNameCtrlQ) noexcept {
    _id = get_unique_id();
    _ctrlq = std::make_shared<MsgQueue>(boost::interprocess::open_only,
                                        daemon_name.c_str());
    _sendq = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                        utils::get_sendq_name(_id).c_str(),
                                        kClientQDepth, sizeof(CtrlMsg));
    _recvq = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                        utils::get_recvq_name(_id).c_str(),
                                        kClientQDepth, sizeof(CtrlMsg));
    connect(daemon_name);
  };
  ~ResourceManager() noexcept {
    disconnect();
    MsgQueue::remove(utils::get_sendq_name(_id).c_str());
    MsgQueue::remove(utils::get_recvq_name(_id).c_str());
  }

  int64_t AllocRegion(size_t size = kPageChunkSize) noexcept;
  int64_t FreeRegion(size_t size = kPageChunkSize) noexcept;
  inline VRange GetRegion(int64_t region_id) noexcept {
    std::unique_lock<std::mutex> lk(_mtx);
    if (_region_map.find(region_id) == _region_map.cend())
      return VRange();
    auto &region = _region_map[region_id];
    return VRange(region->Addr(), region->Size());
  }

  /* A thread safe way to create a global manager and get its reference. */
  static inline ResourceManager *global_manager() noexcept {
    static std::mutex _mtx;
    static std::unique_ptr<ResourceManager> _rmanager(nullptr);

    if (likely(_rmanager.get() != nullptr))
      return _rmanager.get();
    std::unique_lock<std::mutex> lk(_mtx);
    if (unlikely(_rmanager.get() != nullptr))
      return _rmanager.get();

    _rmanager = std::make_unique<ResourceManager>();
    return _rmanager.get();
  }

private:
  int connect(const std::string &daemon_name = kNameCtrlQ) noexcept;
  int disconnect() noexcept;
  size_t free_region(uint64_t region_id) noexcept;

  uint64_t _id;
  std::mutex _mtx;

  std::shared_ptr<MsgQueue> _ctrlq;
  std::shared_ptr<MsgQueue> _sendq;
  std::shared_ptr<MsgQueue> _recvq;

  std::map<int64_t, std::shared_ptr<Region>> _region_map;
};

} // namespace cachebank
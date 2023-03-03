#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <random>

#include "qpair.hpp"
#include "utils.hpp"
#include "shm_types.hpp"

namespace midas {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;
using MappedRegion = boost::interprocess::mapped_region;

class Region {
public:
  Region(uint64_t pid, uint64_t region_id) noexcept;
  ~Region() noexcept;

  inline friend bool operator<(const Region &lhs, const Region &rhs) noexcept {
    return lhs.region_id_ < rhs.region_id_;
  }

  inline void *Addr() const noexcept { return shm_region_->get_address(); }
  inline uint64_t ID() const noexcept { return region_id_; }
  inline int64_t Size() const noexcept { return size_; }

private:
  // generating unique name for the region shared memory file
  uint64_t pid_;
  uint64_t region_id_;
  std::shared_ptr<MappedRegion> shm_region_;
  int64_t size_; // int64_t to adapt to boost::interprocess::offset_t
};

class ResourceManager {
public:
  ResourceManager(const std::string &daemon_name = kNameCtrlQ) noexcept;
  ~ResourceManager() noexcept;

  int64_t AllocRegion(bool overcommit = false) noexcept;
  void FreeRegion(int64_t rid) noexcept;
  void FreeRegions(size_t size = kRegionSize) noexcept;
  inline VRange GetRegion(int64_t region_id) noexcept;

  void UpdateLimit(size_t size) noexcept;

  uint64_t NumRegionInUse() const noexcept;
  uint64_t NumRegionLimit() const noexcept;
  int64_t NumRegionAvail() const noexcept;

  bool reclaim_trigger() const noexcept;

  static inline ResourceManager *global_manager() noexcept;
  static inline std::shared_ptr<ResourceManager>
  global_manager_shared_ptr() noexcept;

private:
  int connect(const std::string &daemon_name = kNameCtrlQ) noexcept;
  int disconnect() noexcept;
  size_t free_region(int64_t region_id) noexcept;

  void pressure_handler();
  void do_update_limit(CtrlMsg &msg);
  void do_reclaim(int64_t nr_to_reclaim);

  uint64_t region_limit_;

  uint64_t id_;
  std::mutex mtx_;

  QPair txqp_;
  QPair rxqp_;

  std::map<int64_t, std::shared_ptr<Region>> region_map_;

  std::shared_ptr<std::thread> handler_thd_;
  bool stop_;
};

} // namespace midas

#include "impl/resource_manager.ipp"
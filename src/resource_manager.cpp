#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <map>
#include <memory>
#include <ratio>
#include <string>
#include <sys/types.h>
#include <thread>

#include "resource_manager.hpp"
#include "utils.hpp"

namespace cachebank {

int ResourceManager::connect(const std::string &daemon_name) noexcept {
  std::unique_lock<std::mutex> lk(_mtx);
  try {
    unsigned int prio = 0;
    size_t recvd_size;
    CtrlMsg msg{.id = _id, .op = CtrlOpCode::CONNECT};

    _ctrlq->send(&msg, sizeof(CtrlMsg), prio);
    _recvq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::CONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection established." << std::endl;
    else {
      std::cerr << "Connection failed." << std::endl;
      exit(1);
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int ResourceManager::disconnect() noexcept {
  std::unique_lock<std::mutex> lk(_mtx);
  try {
    unsigned int prio = 0;
    size_t recvd_size;
    CtrlMsg msg{.id = _id, .op = CtrlOpCode::DISCONNECT};

    _ctrlq->send(&msg, sizeof(CtrlMsg), prio);
    _recvq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::DISCONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection destroyed." << std::endl;
    else {
      std::cerr << "Disconnection failed." << std::endl;
      return -1;
    }
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int64_t ResourceManager::AllocRegion(size_t size) noexcept {
  std::unique_lock<std::mutex> lk(_mtx);
  CtrlMsg msg{.id = _id,
              .op = CtrlOpCode::ALLOC,
              .mmsg = {.size = static_cast<int64_t>(size)}};
  _ctrlq->send(&msg, sizeof(msg), 0);

  unsigned prio;
  size_t recvd_size;
  CtrlMsg ret_msg;
  _recvq->receive(&ret_msg, sizeof(ret_msg), recvd_size, prio);
  if (recvd_size != sizeof(ret_msg)) {
    std::cerr << "ERROR [" << __LINE__ << "] : in recv msg, " << recvd_size
              << " != " << sizeof(ret_msg) << std::endl;
    return -1;
  }
  if (ret_msg.ret != CtrlRetCode::MEM_SUCC) {
    std::cerr << "ERROR in " << __LINE__ << std::endl;
    return -1;
  }

  int64_t region_id = ret_msg.mmsg.region_id;
  assert(_region_map.find(region_id) == _region_map.cend());

  auto region = std::make_shared<Region>(_id, region_id);
  _region_map[region_id] = region;
  assert(region->Size() == ret_msg.mmsg.size);
  assert((reinterpret_cast<uint64_t>(region->Addr()) & kPageChunkAlignMask) ==
         0);

  std::cout << "Allocated a page chunk: " << region->Addr() << " ["
            << region->Size() << "]" << std::endl;
  return region_id;
}

int64_t ResourceManager::FreeRegion(size_t size) noexcept {
  std::unique_lock<std::mutex> lk(_mtx);
  size_t total_freed = 0;
  int nr_freed_chunks = 0;
  while (!_region_map.empty()) {
    auto region_iter = _region_map.begin();
    size_t freed_bytes = free_region(region_iter->second->ID());
    total_freed += freed_bytes;
    nr_freed_chunks++;
    if (total_freed >= size)
      break;
  }
  std::cout << "Freed " << nr_freed_chunks << " page chunks (" << total_freed
            << "bytes)" << std::endl;
  return 0;
}

/** This function is supposed to be called inside a locked section */
inline size_t ResourceManager::free_region(uint64_t region_id) noexcept {
  size_t size = 0;
  auto region_iter = _region_map.find(region_id);
  if (region_iter == _region_map.cend()) {
    std::cerr << "Invalid region_id " << region_id << std::endl;
    return -1;
  }

  _region_map.erase(region_id);
  size = region_iter->second->Size();
  std::cout << "page_chunk_map size: " << _region_map.size() << std::endl;
  return size;
}

} // namespace cachebank

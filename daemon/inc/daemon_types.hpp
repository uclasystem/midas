#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstdint>
#include <unordered_map>
#include <utility>

#include "qpair.hpp"
#include "shm_types.hpp"

namespace cachebank {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;

enum class ClientStatusCode {
  INIT,
  CONNECTED,
  DISCONNECTED,
};

class Client {
public:
  Client(uint64_t id_);
  ~Client();

  uint64_t id;
  ClientStatusCode status;
  QSingle cq; // per-client completion queue for the ctrl queue
  QPair txqp;
  std::unordered_map<int64_t, std::shared_ptr<SharedMemObj>> regions;

private:
  friend class Daemon;

  inline int64_t new_region_id_() noexcept;
  inline void unmap_regions_();

  void connect();
  void disconnect();
  void alloc_region(size_t size);
  void free_region(int64_t region_id);

  uint64_t region_cnt_;
};

class Daemon {
public:
  Daemon(const std::string ctrlq_name = kNameCtrlQ);
  ~Daemon();
  void serve();

  static Daemon *get_daemon();

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);

  const std::string ctrlq_name_;
  std::shared_ptr<MsgQueue> ctrlq_;
  std::unordered_map<uint64_t, Client> clients_;
};

} // namespace cachebank

#include "impl/daemon_types.ipp"
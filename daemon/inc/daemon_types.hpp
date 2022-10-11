#pragma once

#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstdint>
#include <unordered_map>
#include <utility>

#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

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
  Client(uint64_t id_, uint64_t region_limit = -1ull);
  ~Client();

  uint64_t id;
  ClientStatusCode status;

  void connect();
  void disconnect();
  void alloc_region(size_t size);
  void overcommit_region(size_t size);
  void free_region(int64_t region_id);
  void reclaim_regions(uint64_t mem_limit);

private:
  inline int64_t new_region_id_() noexcept;
  inline void destroy();

  void alloc_region_(size_t size, bool overcommit);

  QSingle cq; // per-client completion queue for the ctrl queue
  QPair txqp;
  std::unordered_map<int64_t, std::shared_ptr<SharedMemObj>> regions;

  uint64_t region_cnt_;
  uint64_t region_limit_;
};

class Daemon {
public:
  Daemon(const std::string cfg_file = kDaemonCfgFile,
         const std::string ctrlq_name = kNameCtrlQ);
  ~Daemon();
  void serve();

  static Daemon *get_daemon();

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_overcommit(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);

  void monitor();

  const std::string ctrlq_name_;
  std::shared_ptr<MsgQueue> ctrlq_;
  std::unordered_map<uint64_t, Client> clients_;

  uint64_t mem_limit_;
  std::string cfg_file_;
  constexpr static uint64_t kRegionLimit = (1ull << 30) / kRegionSize; // 1GB
  constexpr static char kDaemonCfgFile[] = "config/mem.config";
};

} // namespace cachebank

#include "impl/daemon_types.ipp"
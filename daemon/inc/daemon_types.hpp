#pragma once

#include <atomic>
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/permissions.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>

#include "qpair.hpp"
#include "shm_types.hpp"
#include "utils.hpp"

namespace midas {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;

enum class ClientStatusCode {
  INIT,
  CONNECTED,
  DISCONNECTED,
};

struct CacheStats {
  uint64_t hits{0};
  uint64_t misses{0};
  double penalty{0.};
  uint64_t vhits{0};
  uint64_t vcache_size{0};
  double perf_gain{0.};
};

class Daemon;

class Client {
public:
  Client(Daemon *daemon, uint64_t id_, uint64_t region_limit);
  ~Client();

  uint64_t id;
  ClientStatusCode status;

  void connect();
  void disconnect();
  bool alloc_region();
  bool overcommit_region();
  bool free_region(int64_t region_id);
  void update_limit(uint64_t mem_limit);
  bool profile_stats();

private:
  inline int64_t new_region_id_() noexcept;
  inline void destroy();

  bool alloc_region_(bool overcommit);

  std::mutex tx_mtx;
  QSingle cq; // per-client completion queue for the ctrl queue
  QPair txqp;
  std::unordered_map<int64_t, std::shared_ptr<SharedMemObj>> regions;

  CacheStats stats;

  Daemon *daemon_;
  uint64_t region_cnt_;
  uint64_t region_limit_;

  friend class Daemon;
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
  int do_update_limit_req(const CtrlMsg &msg);

  void charge(int64_t nr_regions);
  void uncharge(int64_t nr_regions);

  void monitor();
  void profiler();
  void rebalancer();

  void on_mem_shrink();
  void on_mem_expand();
  void on_mem_rebalance();

  enum class MemStatus {
    NORMAL,
    NEED_SHRINK,
    NEED_EXPAND,
    NEED_REBALANCE
  } status_;
  std::mutex rbl_mtx_;
  std::condition_variable rbl_cv_;

  bool terminated_;
  std::shared_ptr<std::thread> monitor_;
  std::shared_ptr<std::thread> profiler_;
  std::shared_ptr<std::thread> rebalancer_;

  const std::string ctrlq_name_;
  std::shared_ptr<MsgQueue> ctrlq_;
  std::mutex mtx_;
  std::unordered_map<uint64_t, std::unique_ptr<Client>> clients_;

  std::atomic_int_fast64_t region_cnt_;
  uint64_t region_limit_;
  // For simluation
  std::string cfg_file_;
  constexpr static uint64_t kInitRegions = (100ull << 20) / kRegionSize; // 100MB
  constexpr static uint64_t kMaxRegions = (100ull << 30) / kRegionSize; // 100GB
  constexpr static char kDaemonCfgFile[] = "config/mem.config";

  friend class Client;
};

} // namespace midas

#include "impl/daemon_types.ipp"
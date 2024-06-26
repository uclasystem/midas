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
  int32_t headroom{0};
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
  bool update_limit(uint64_t new_limit);
  void set_weight(float weight);
  void set_lat_critical(bool value);
  bool force_reclaim(uint64_t new_limit);
  bool profile_stats();
  bool almost_full() noexcept;

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
  float weight_;
  int32_t warmup_ttl_;
  bool lat_critical_;

  friend class Daemon;
};

class Daemon {
public:
  Daemon(const std::string ctrlq_name = kNameCtrlQ);
  ~Daemon();
  void serve();

  static Daemon *get_daemon();

  enum Policy {
    Invalid = -1,
    Static = 0,
    Midas,
    CliffHanger,
    RobinHood,
    ExpandOnly,
    NumPolicy
  };

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_overcommit(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);
  int do_update_limit_req(const CtrlMsg &msg);
  int do_set_weight(const CtrlMsg &msg);
  int do_set_lat_critical(const CtrlMsg &msg);

  void charge(int64_t nr_regions);
  void uncharge(int64_t nr_regions);

  void monitor();
  void profiler();
  void rebalancer();

  void on_mem_shrink();
  void on_mem_expand();
  void on_mem_rebalance();
  void on_mem_rebalance_cliffhanger();

  enum class MemStatus {
    NORMAL,
    NEED_SHRINK,
    NEED_EXPAND,
    NEED_REBALANCE
  } status_;
  std::mutex rbl_mtx_;
  std::condition_variable rbl_cv_;

  bool terminated_;
  Policy policy;
  std::shared_ptr<std::thread> monitor_;
  std::shared_ptr<std::thread> profiler_;
  std::shared_ptr<std::thread> rebalancer_;

  const std::string ctrlq_name_;
  QSingle ctrlq_;
  std::mutex mtx_;
  std::unordered_map<uint64_t, std::shared_ptr<Client>> clients_;

  std::atomic_int_fast64_t region_cnt_;
  uint64_t region_limit_;
  // For simluation
  constexpr static uint64_t kInitRegions = (100ull << 20) / kRegionSize;// 100MB
  constexpr static uint64_t kMaxRegions = (100ull << 30) / kRegionSize; // 100GB

  friend class Client;
};

namespace utils {
uint64_t check_sys_avail_mem();
uint64_t check_file_avail_mem();

Daemon::Policy check_policy();

constexpr static char kMemoryCfgFile[] = "config/mem.config";
constexpr static char kPolicyCfgFile[] = "config/policy.config";
}

} // namespace midas

#include "impl/daemon_types.ipp"
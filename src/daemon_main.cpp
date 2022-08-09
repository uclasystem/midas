#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <unordered_map>
#include <utility>

#include "utils.hpp"

namespace cachebank {

using SharedMemObj = boost::interprocess::shared_memory_object;
using MsgQueue = boost::interprocess::message_queue;

class Daemon;

class Client {
public:
  Client(uint64_t id_)
      : status(ClientStatusCode::INIT), id(id_), _region_cnt(0) {
    connect();
  }
  ~Client() { unmap_regions(); }

  uint64_t id;
  ClientStatusCode status;
  std::shared_ptr<MsgQueue> tx_conn;
  std::shared_ptr<MsgQueue> rx_conn;
  std::unordered_map<uint64_t, std::shared_ptr<SharedMemObj>> regions;

private:
  friend class Daemon;
  inline uint64_t new_region_id() noexcept { return _region_cnt++; }
  void connect();
  std::pair<CtrlRetCode, MemMsg> alloc_region(size_t size);
  std::pair<CtrlRetCode, MemMsg> free_region(int64_t region_id);
  void unmap_regions();

  uint64_t _region_cnt;
};

void Client::connect() {
  /** Daemon connects its tx queue to the client's recvq, and its rx queue to
   * the client's sendq, respectively. */
  tx_conn = std::make_shared<MsgQueue>(boost::interprocess::open_only,
                                       utils::get_recvq_name(id).c_str());
  rx_conn = std::make_shared<MsgQueue>(boost::interprocess::open_only,
                                       utils::get_sendq_name(id).c_str());
}

std::pair<CtrlRetCode, MemMsg> Client::alloc_region(size_t size) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;

  int64_t region_id = new_region_id();
  const auto rwmode = boost::interprocess::read_write;
  const std::string chunkname = utils::get_region_name(id, region_id);

  if (regions.find(region_id) == regions.cend()) {
    int64_t actual_size;
    auto region = std::make_shared<SharedMemObj>(
        boost::interprocess::create_only, chunkname.c_str(), rwmode);
    regions.insert(std::make_pair(region_id, region));
    region->truncate(size);
    region->get_size(actual_size);

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    std::cerr << "Client " << id << " has already allocated region "
              << region_id << std::endl;

    ret = CtrlRetCode::MEM_FAIL;
  }
  return std::make_pair(ret, std::move(mm));
}

std::pair<CtrlRetCode, MemMsg> Client::free_region(int64_t region_id) {
  CtrlRetCode ret = CtrlRetCode::MEM_FAIL;
  MemMsg mm;
  int64_t actual_size;

  auto region_iter = regions.find(region_id);
  assert(region_iter != regions.cend());
  region_iter->second->get_size(actual_size);
  regions.erase(region_id);

  if (regions.find(region_id) != regions.cend()) {
    /* Successfully find the region to be freed */
    regions.erase(region_id);

    ret = CtrlRetCode::MEM_SUCC;
    mm.region_id = region_id;
    mm.size = actual_size;
  } else {
    /* Failed to find corresponding region */
    std::cerr << "Client " << id << " doesn't have region " << region_id
              << std::endl;
    ret = CtrlRetCode::MEM_FAIL;
  }

  return std::make_pair(ret, std::move(mm));
}

void Client::unmap_regions() {
  for (const auto &kv : regions) {
    const std::string name = utils::get_region_name(id, kv.first);
    SharedMemObj::remove(name.c_str());
  }
}

class Daemon {
public:
  Daemon(const std::string ctrlq_name = kNameCtrlQ) : _ctrlq_name(ctrlq_name) {
    MsgQueue::remove(_ctrlq_name.c_str());
    _ctrlq = std::make_shared<MsgQueue>(boost::interprocess::create_only,
                                        ctrlq_name.c_str(), kDaemonQDepth,
                                        sizeof(CtrlMsg));
  }
  ~Daemon() { MsgQueue::remove(_ctrlq_name.c_str()); }
  void serve();

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);

  const std::string _ctrlq_name;
  std::shared_ptr<MsgQueue> _ctrlq;
  std::unordered_map<uint64_t, Client> _clients;
};

int Daemon::do_connect(const CtrlMsg &msg) {
  try {
    if (_clients.find(msg.id) != _clients.cend()) {
      std::cerr << "Client " << msg.id << " connected twice!" << std::endl;
      /* TODO: this might be some stale client. Probably we could try to
       * send the ack back */
      return -1;
    }
    // _clients[msg.id] = std::move(Client(msg.id));
    _clients.insert(std::make_pair(msg.id, Client(msg.id)));
    auto client_iter = _clients.find(msg.id);
    assert(client_iter != _clients.cend());
    auto &client = client_iter->second;
    std::cout << "Client " << msg.id << " connected." << std::endl;

    CtrlMsg ack{.op = CtrlOpCode::CONNECT, .ret = CtrlRetCode::CONN_SUCC};
    client.tx_conn->send(&ack, sizeof(ack), 0);
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int Daemon::do_disconnect(const CtrlMsg &msg) {
  try {
    auto client = _clients.find(msg.id);
    if (client == _clients.cend()) {
      /* TODO: this might be some unregistered client. Probably we could try to
       * connect to it and send the ack back */
      std::cerr << "Client " << msg.id << " didn't exist!" << std::endl;
      return -1;
    }

    CtrlMsg ret_msg{.op = CtrlOpCode::DISCONNECT,
                    .ret = CtrlRetCode::CONN_SUCC};
    client->second.tx_conn->send(&ret_msg, sizeof(ret_msg), 0);

    _clients.erase(msg.id);
    std::cout << "Client " << msg.id << " disconnected!" << std::endl;
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int Daemon::do_alloc(const CtrlMsg &msg) {
  assert(msg.mmsg.size != 0);
  auto client_iter = _clients.find(msg.id);
  if (client_iter == _clients.cend()) {
    /* TODO: same as in do_disconnect */
    std::cerr << "Client " << msg.id << " doesn't exist!" << std::endl;
    return -1;
  }
  auto &client = client_iter->second;
  assert(msg.id == client.id);

  auto ret_mmsg = client.alloc_region(msg.mmsg.size);
  CtrlMsg ret_msg{
      .op = CtrlOpCode::ALLOC, .ret = ret_mmsg.first, .mmsg = ret_mmsg.second};
  client.tx_conn->send(&ret_msg, sizeof(ret_msg), 0);

  return 0;
}

int Daemon::do_free(const CtrlMsg &msg) {
  auto client_iter = _clients.find(msg.id);
  if (client_iter == _clients.cend()) {
    /* TODO: same as in do_disconnect */
    std::cerr << "Client " << msg.id << " doesn't exist!" << std::endl;
    return -1;
  }

  uint64_t region_id = msg.mmsg.region_id;
  size_t region_size = msg.mmsg.size;
  const std::string chunkname = utils::get_region_name(msg.id, region_id);
  auto &client = client_iter->second;

  auto ret_mmsg = client.free_region(region_id);
  CtrlMsg ack{
      .op = CtrlOpCode::FREE, .ret = ret_mmsg.first, .mmsg = ret_mmsg.second};
  client.tx_conn->send(&ack, sizeof(ack), 0);

  return 0;
}

void Daemon::serve() {
  std::cout << "Daemon starts listening..." << std::endl;

  while (true) {
    CtrlMsg msg;
    size_t recvd_size;
    unsigned int prio;

    _ctrlq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      break;
    }
    std::cout << "Daemon recved msg " << msg.op << std::endl;
    switch (msg.op) {
    case CONNECT:
      do_connect(msg);
      break;
    case DISCONNECT:
      do_disconnect(msg);
      break;
    case ALLOC:
      do_alloc(msg);
      break;
    case FREE:
      do_free(msg);
      break;
    default:
      std::cerr << "Recved unknown message: " << msg.op << std::endl;
    }
  }
}
} // namespace cachebank

int main(int argc, char *argv[]) {
  cachebank::Daemon daemon;
  daemon.serve();

  return 0;
}
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

class Client {
public:
  Client(uint64_t id) : _id(id), status(ClientStatusCode::INIT) { connect(); }

  ClientStatusCode status;
  std::shared_ptr<boost::interprocess::message_queue> tx_conn;
  std::shared_ptr<boost::interprocess::message_queue> rx_conn;
private:
  void connect();

  uint64_t _id;
};

void Client::connect() {
  /** Daemon connects its tx queue to the client's recvq, and its rx queue to
   * the client's sendq, respectively. */
  tx_conn = std::make_shared<boost::interprocess::message_queue>(
      boost::interprocess::open_only, get_recvq_name(_id).c_str());
  rx_conn = std::make_shared<boost::interprocess::message_queue>(
      boost::interprocess::open_only, get_sendq_name(_id).c_str());
}

class Daemon {
public:
  Daemon(const std::string ctrlq_name = kNameCtrlQ) : _ctrlq_name(ctrlq_name) {
    boost::interprocess::message_queue::remove(_ctrlq_name.c_str());
    _ctrlq = std::make_shared<boost::interprocess::message_queue>(
        boost::interprocess::create_only, ctrlq_name.c_str(), kDaemonQDepth,
        sizeof(CtrlMsg));
  }
  ~Daemon() { boost::interprocess::message_queue::remove(_ctrlq_name.c_str()); }
  void serve();

private:
  int do_connect(const CtrlMsg &msg);
  int do_disconnect(const CtrlMsg &msg);
  int do_alloc(const CtrlMsg &msg);
  int do_free(const CtrlMsg &msg);

  const std::string _ctrlq_name;
  std::shared_ptr<boost::interprocess::message_queue> _ctrlq;
  std::unordered_map<uint64_t, Client> _clients;
};

int Daemon::do_connect(const CtrlMsg &msg) {
  try {
    if (_clients.find(msg.id) != _clients.end()) {
      std::cerr << "Client " << msg.id << " connected twice!" << std::endl;
      return -1;
    }
    // _clients[msg.id] = std::move(Client(msg.id));
    _clients.insert(std::make_pair(msg.id, Client(msg.id)));
    auto client = _clients.find(msg.id);
    assert(client != _clients.end());
    std::cout << "Client " << msg.id << " connected." << std::endl;

    CtrlMsg ret_msg { .op = CtrlOpCode::CONNECT, .ret = CtrlRetCode::CONN_SUCC };
    client->second.tx_conn->send(&ret_msg, sizeof(ret_msg), 0);
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int Daemon::do_disconnect(const CtrlMsg &msg) {
  try {
    auto client = _clients.find(msg.id);
    if (client == _clients.cend()) {
      std::cerr << "Client " << msg.id << " didn't exist!" << std::endl;
      return -1;
    }

    CtrlMsg ret_msg { .op = CtrlOpCode::DISCONNECT, .ret = CtrlRetCode::CONN_SUCC };
    client->second.tx_conn->send(&ret_msg, sizeof(ret_msg), 0);

    _clients.erase(msg.id);
    std::cout << "Client " << msg.id << " disconnected!" << std::endl;
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int Daemon::do_alloc(const CtrlMsg &msg) { return 0; }

int Daemon::do_free(const CtrlMsg &msg) { return 0; }

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

int main(int argc, char *argv[]) {
  Daemon daemon;
  daemon.serve();

  return 0;
}
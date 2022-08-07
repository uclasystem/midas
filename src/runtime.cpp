#include "boost/interprocess/detail/os_thread_functions.hpp"
#include "boost/interprocess/exceptions.hpp"
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <ratio>
#include <string>
#include <sys/types.h>
#include <thread>

#include "utils.hpp"

using namespace boost::interprocess;

static inline uint64_t get_unique_id() {
  auto pid = ipcdetail::get_current_process_id();
  auto creation_time = ipcdetail::get_current_process_creation_time();

  return static_cast<uint64_t>(pid);
}

class Client {
public:
  Client(const std::string &daemon_name = kNameCtrlQ) {
    connect(daemon_name);
  };
  ~Client() {
    disconnect();
  }
private:
  int connect(const std::string &daemon_name = kNameCtrlQ);
  int disconnect();

  uint64_t id;
  std::shared_ptr<message_queue> ctrlq;
  std::shared_ptr<message_queue> sendq;
  std::shared_ptr<message_queue> recvq;
};

int Client::connect(const std::string &daemon_name) {
  id = get_unique_id();
  try {

    // auto ctime = ipcdetail::get_current_process_creation_time();
    ctrlq = std::make_shared<message_queue>(open_only, daemon_name.c_str());
    sendq = std::make_shared<message_queue>(create_only, get_sendq_name(id).c_str(), kClientQDepth, sizeof(CtrlMsg));
    recvq = std::make_shared<message_queue>(create_only, get_recvq_name(id).c_str(), kClientQDepth, sizeof(CtrlMsg));

    unsigned int prio = 0;
    CtrlMsg msg;
    msg.pid = ipcdetail::get_current_process_id();
    msg.op = CtrlOpCode::CONNECT;
    ctrlq->send(&msg, sizeof(CtrlMsg), prio);

    size_t recvd_size;
    recvq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::CONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection established." << std::endl;
    else {
      std::cerr << "Connection failed." << std::endl;
      exit(1);
    }
  } catch (interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int Client::disconnect() {
  try{
    unsigned int prio = 0;
    CtrlMsg msg;
    msg.pid = id;
    msg.op = CtrlOpCode::DISCONNECT;
    ctrlq->send(&msg, sizeof(CtrlMsg), prio);

    size_t recvd_size;
    recvq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::DISCONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection destroyed." << std::endl;
    else {
      std::cerr << "Disconnection failed." << std::endl;
      exit(1);
    }
    message_queue::remove(get_sendq_name(id).c_str());
    message_queue::remove(get_recvq_name(id).c_str());
  } catch (interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int main(int argc, char *argv[]) {
  Client client;
  return 0;
}
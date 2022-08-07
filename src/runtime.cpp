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

static inline uint64_t get_unique_id() {
  auto pid = boost::interprocess::ipcdetail::get_current_process_id();
  auto creation_time = boost::interprocess::ipcdetail::get_current_process_creation_time();

  return static_cast<uint64_t>(pid);
}

class ResourceManager {
public:
  ResourceManager(const std::string &daemon_name = kNameCtrlQ) {
    id = get_unique_id();
    ctrlq = std::make_shared<boost::interprocess::message_queue>(
        boost::interprocess::open_only, daemon_name.c_str());
    sendq = std::make_shared<boost::interprocess::message_queue>(
        boost::interprocess::create_only, get_sendq_name(id).c_str(), kClientQDepth,
        sizeof(CtrlMsg));
    recvq = std::make_shared<boost::interprocess::message_queue>(
        boost::interprocess::create_only, get_recvq_name(id).c_str(), kClientQDepth,
        sizeof(CtrlMsg));
    connect(daemon_name);
  };
  ~ResourceManager() {
    disconnect();
    boost::interprocess::message_queue::remove(get_sendq_name(id).c_str());
    boost::interprocess::message_queue::remove(get_recvq_name(id).c_str());
  }
private:
  int connect(const std::string &daemon_name = kNameCtrlQ);
  int disconnect();

  uint64_t id;
  std::shared_ptr<boost::interprocess::message_queue> ctrlq;
  std::shared_ptr<boost::interprocess::message_queue> sendq;
  std::shared_ptr<boost::interprocess::message_queue> recvq;
};

int ResourceManager::connect(const std::string &daemon_name) {
  try {
    unsigned int prio = 0;
    size_t recvd_size;
    CtrlMsg msg {.id = id, .op = CtrlOpCode::CONNECT };

    ctrlq->send(&msg, sizeof(CtrlMsg), prio);
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
  } catch (boost::interprocess::interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int ResourceManager::disconnect() {
  try{
    unsigned int prio = 0;
    size_t recvd_size;
    CtrlMsg msg { .id = id, .op = CtrlOpCode::DISCONNECT };

    ctrlq->send(&msg, sizeof(CtrlMsg), prio);
    recvq->receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
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

int main(int argc, char *argv[]) {
  ResourceManager rmanager;
  return 0;
}
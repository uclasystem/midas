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

int connect() {
  try {
    message_queue ctrl_q(open_only, kNameCtrlQ);

    uint64_t id = get_unique_id();
    // auto ctime = ipcdetail::get_current_process_creation_time();
    message_queue send_q(create_only, ("send-" + std::to_string(id)).c_str(), kClientQDepth, sizeof(CtrlMsg));
    message_queue recv_q(create_only, ("recv-" + std::to_string(id)).c_str(), kClientQDepth, sizeof(CtrlMsg));

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    unsigned int prio = 0;
    CtrlMsg msg;
    msg.pid = ipcdetail::get_current_process_id();
    msg.op = CtrlOpCode::CONNECT;

    ctrl_q.send(&msg, sizeof(CtrlMsg), prio);

    size_t recvd_size;
    recv_q.receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::CONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection established." << std::endl;
    else {
      std::cerr << "Connection failed." << std::endl;
      exit(1);
    }

    msg.pid = ipcdetail::get_current_process_id();
    msg.op = CtrlOpCode::DISCONNECT;
    ctrl_q.send(&msg, sizeof(CtrlMsg), prio);

    recv_q.receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
    if (recvd_size != sizeof(CtrlMsg)) {
      return -1;
    }
    if (msg.op == CtrlOpCode::DISCONNECT && msg.ret == CtrlRetCode::CONN_SUCC)
      std::cout << "Connection destroyed." << std::endl;
    else {
      std::cerr << "Disconnection failed." << std::endl;
      exit(1);
    }

  } catch (interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int main(int argc, char *argv[]) {
  int ret = connect();
  return 0;
}
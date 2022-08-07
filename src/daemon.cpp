#include "boost/interprocess/detail/os_thread_functions.hpp"
#include "boost/interprocess/exceptions.hpp"
#include <boost/interprocess/ipc/message_queue.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/shared_memory_object.hpp>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <set>
#include <string>
#include <sys/types.h>
#include <unistd.h>

using namespace boost::interprocess;

#include "utils.hpp"

std::set<pid_t> clients;

int do_connect(const CtrlMsg &msg) {
  try {
    std::string q_name = "recv-" + std::to_string(msg.pid);
    message_queue q(open_only, q_name.c_str());

    if (clients.find(msg.pid) != clients.cend()) {
      std::cerr << "Client " << msg.pid << " connected twice!" << std::endl;
    }
    clients.insert(msg.pid);
    std::cerr << "Client " << msg.pid << " connected." << std::endl;

    CtrlMsg msg;
    msg.op = CtrlOpCode::CONNECT;
    msg.ret = CtrlRetCode::CONN_SUCC;

    q.send(&msg, sizeof(msg), 0);
  } catch (interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int do_disconnect(const CtrlMsg &msg) {
  try {
    std::string q_name = "recv-" + std::to_string(msg.pid);
    message_queue q(open_only, q_name.c_str());

    auto client = clients.find(msg.pid);
    if (client != clients.cend()) {
      clients.erase(msg.pid);
      std::cout << "Client " << msg.pid << " disconnected!" << std::endl;
    } else {
      std::cerr << "Client " << msg.pid << " didn't exist!" << std::endl;
    }

    CtrlMsg n_msg;
    n_msg.pid = msg.pid;
    n_msg.op = CtrlOpCode::DISCONNECT;
    n_msg.ret = CtrlRetCode::CONN_SUCC;
    q.send(&n_msg, sizeof(n_msg), 0);
  } catch (interprocess_exception &e) {
    std::cerr << e.what() << std::endl;
  }

  return 0;
}

int do_alloc(const CtrlMsg &msg) {
  return 0;
}

int do_free(const CtrlMsg &msg) {
  return 0;
}

int server() {
  struct shm_remove {
    shm_remove() { shared_memory_object::remove(kNameCtrlQ); }
    ~shm_remove() { shared_memory_object::remove(kNameCtrlQ); }
  } remover;
  message_queue mq(open_or_create, kNameCtrlQ, kDaemonQDepth, sizeof(CtrlMsg));

  // ipcdetail::OS_process_id_t id = ipcdetail::get_current_process_id();
  size_t recvd_size;
  unsigned int prio;
  CtrlMsg msg;

  std::cout << "Daemon starts listening..." << std::endl;

  while (true) {
    mq.receive(&msg, sizeof(CtrlMsg), recvd_size, prio);
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
  return 0;
}


int main(int argc, char *argv[]) {
  server();

  return 0;
}
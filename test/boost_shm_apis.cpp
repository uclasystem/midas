
int ipc() {
  try {
    // {
    //   message_queue t_mq(create_only,
    //                   "mq"       // name
    //                   , 20, sizeof(int)
    //   );
    // }

    // opening the message queue whose name is mq
    message_queue mq(open_only, // only open
                     "mq"       // name
    );
    size_t recvd_size;
    unsigned int priority = 0;

    for (int i = 0; i < 20; ++i) {
      mq.send((void *)&i, sizeof(int), priority);
    }

    // now send the messages to the queue
    for (int i = 0; i < 20; ++i) {
      int buffer;
      mq.receive((void *)&buffer, sizeof(int), recvd_size, priority);
      if (recvd_size != sizeof(int))
        ; // do the error handling
      std::cout << buffer << " " << recvd_size << " " << priority << std::endl;
    }
  } catch (interprocess_exception &e) {
    std::cout << e.what() << std::endl;
  }

  return 0;
}

int shm(int argc, char *argv[]) {
  if (argc == 1) { // Parent process
    // Remove shared memory on construction and destruction
    struct shm_remove {
      shm_remove() { shared_memory_object::remove("MySharedMemory"); }
      ~shm_remove() { shared_memory_object::remove("MySharedMemory"); }
    } remover;

    // Create a shared memory object.
    shared_memory_object shm(create_only, "MySharedMemory", read_write);

    // Set size
    shm.truncate(1000);

    // Map the whole shared memory in this process
    mapped_region region(shm, read_write);

    // Write all the memory to 1
    std::memset(region.get_address(), 1, region.get_size());

    // Launch child process
    std::string s(argv[0]);
    s += " child ";
    if (0 != std::system(s.c_str()))
      return 1;
  } else {
    // Open already created shared memory object.
    shared_memory_object shm(open_only, "MySharedMemory", read_only);

    // Map the whole shared memory in this process
    mapped_region region(shm, read_only);

    // Check that memory was initialized to 1
    char *mem = static_cast<char *>(region.get_address());
    for (std::size_t i = 0; i < region.get_size(); ++i)
      if (*mem++ != 1)
        return 1; // Error checking memory
  }
  return 0;
}
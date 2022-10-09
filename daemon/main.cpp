#include <csignal>

#include "inc/daemon_types.hpp"

void signalHandler(int signum) {
  // Let the process exit normally so that daemon_ can be naturally destroyed.
  exit(signum);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, signalHandler);

  auto daemon = cachebank::Daemon::get_daemon();
  daemon->serve();

  return 0;
}
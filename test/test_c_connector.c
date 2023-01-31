#include <stdio.h>
#include <unistd.h>

#include <midas.h>

int main(int argc, char *argv[]) {
  void *rmanager = midas_get_global_manager();
  printf("get midas resource manager @ %p\n", rmanager);
  sleep(1); // leave some time for rmanager to establish connections.
  return 0;
}
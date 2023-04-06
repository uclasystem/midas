#include "rh_nuapp.hpp"
#include "requestor.hpp"

int main(int argc, char *argv[]) {
  onerf::RHAppServer app_server;
  onerf::Requestor requestor(&app_server);

  for (int i = 1; i <= 10; i++)
    requestor.Perf(i);

  return 0;
}
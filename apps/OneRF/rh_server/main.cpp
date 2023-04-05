#include "rh_nuapp.hpp"
#include "requestor.hpp"

int main(int argc, char *argv[]) {
  onerf::RHAppServer app_server;
  onerf::Requestor requestor(&app_server);

  requestor.Perf();

  return 0;
}
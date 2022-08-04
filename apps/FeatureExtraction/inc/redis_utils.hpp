#pragma once

#include <sw/redis++/cxx_utils.h>
#include <sw/redis++/redis++.h>

using namespace sw::redis;

inline Redis init_redis_client_pool() {
  std::string host = "localhost";
  int port = 6379;
  int connections = 512;
  int timeout_ms = 10000;
  int keepalive_ms = 10000;

  ConnectionOptions connection_options;
  connection_options.host = host;
  connection_options.port = port;

  ConnectionPoolOptions pool_options;
  pool_options.size = connections;
  pool_options.wait_timeout = std::chrono::milliseconds(timeout_ms);
  pool_options.connection_lifetime = std::chrono::milliseconds(keepalive_ms);

  return Redis(connection_options, pool_options);
}

#pragma once

#include <cstring>
#include <memory>
#include <mutex>
#include <sw/redis++/cxx_utils.h>
#include <sw/redis++/redis++.h>

using namespace sw::redis;

static inline Redis *global_redis() {
  static std::mutex mtx_;
  static std::unique_ptr<Redis> redis_;

  if (redis_)
    return redis_.get();
  std::unique_lock<std::mutex> ul(mtx_);
  if (!redis_) {
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
    redis_ = std::make_unique<Redis>(connection_options, pool_options);
  }
  return redis_.get();
}

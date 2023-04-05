#pragma once

#include "query_types.hpp"

namespace onerf {
class AppServer {
public:
  virtual ~AppServer() = default;
  virtual bool ParseRequest(const QueryLayerSlice &layers) = 0;
};
}
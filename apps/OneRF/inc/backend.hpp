#pragma once

#include <map>
#include <string>
#include <vector>

namespace onerf {
using Item = std::basic_string<int8_t>;

class Backend {
public:
  virtual ~Backend() = default;

  virtual bool Request(const std::vector<std::string> &ids,
                       std::map<std::string, Item> &res_map) = 0;
};
} // namespace onerf
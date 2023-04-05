#pragma once

#include <map>
#include <string>
#include <vector>

#include "backend.hpp"

namespace onerf {
class MySqlBack : public Backend {
public:
  MySqlBack();
  ~MySqlBack() override;
  bool Request(const std::vector<std::string> &ids,
               std::map<std::string, Item> &ret_map) override;

private:
  void *data;
};
} // namespace onerf
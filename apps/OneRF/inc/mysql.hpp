#pragma once

#define DISABLE_MYSQL_BACK
#ifndef DISABLE_MYSQL_BACK

#include <map>
#include <string>
#include <vector>
#include <memory>

#include "backend.hpp"
#include <mysqlx/xdevapi.h>

namespace onerf {
class MySqlBack : public Backend {
public:
  MySqlBack(int port = 33000);
  ~MySqlBack() override;
  bool Request(const std::vector<std::string> &ids,
               std::map<std::string, Item> &ret_map) override;
  void connect();
  void disconnect();

private:
  std::unique_ptr<mysqlx::Session> session_;
  // std::unique_ptr<mysqlx::Schema> db_;
  int port_;

  const std::string kDBUser = "onerf";
  const std::string kDBPasswd = "onerf";
  const std::string kDBName = "onerfdb";
};
} // namespace onerf

#endif // DISABLE_MYSQL_BACK
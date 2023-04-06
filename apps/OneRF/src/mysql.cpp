#include "mysql.hpp"
#include <iostream>
#include <memory>

#ifndef DISABLE_MYSQL_BACK

namespace onerf {
MySqlBack::MySqlBack(int port) : port_(port) {
  connect();
}

MySqlBack::~MySqlBack() { disconnect(); }

bool MySqlBack::Request(const std::vector<std::string> &ids,
                        std::map<std::string, Item> &ret_map) {
  if (ids.empty()) {
    std::cerr << "Empty mysql ids" << std::endl;
    return false;
  }

  // Execute a query
  std::string query;
  if (!ids.empty()) {
    query = "SELECT oid, value FROM blobs WHERE oid in (?";
    for (int i = 0; i < ids.size(); i++)
      query += ",?";
    query += ") LIMIT " + std::to_string(ids.size());
  } else {
    query = "SELECT oid, value FROM blobs WHERE oid=? LIMIT 1";
  }
  mysqlx::SqlResult result = session_->sql(query).execute();

  // Fetch the results
  auto &columns = result.getColumns();
  std::cout << "Columns: ";
  for (auto & col : columns) {
    std::cout << col.getColumnName() << " ";
  }
  std::cout << std::endl;
  std::cout << "Results:" << std::endl;
  while (mysqlx::Row row = result.fetchOne()) {
    std::cout << row[0].get<int>() << " " << row[1].get<std::string>()
              << std::endl;
  }
  return true;
}

void MySqlBack::connect() {
  // Connect to the database
  // std::string uri = "mysqlx://" + kDBUser + ":" + kDBPasswd +
  //                   "@localhost:" + std::to_string(port_) + "/" + kDBName;
  // + "?timeout=10s&writeTimeout=10s&readTimeout=10s";
  std::string uri = "mysqlx://" + kDBUser + ":" + kDBPasswd +
                    "@localhost:" + "/" + kDBName;
  std::cout << "Connect to " << uri << std::endl;
  session_ = std::make_unique<mysqlx::Session>(uri);
  // db_ = std::unique_ptr(session_->getSchema("mydb"));
}

void MySqlBack::disconnect() {
  if (session_)
    session_->close();
}
} // namespace onerf

#endif // DISABLE_MYSQL_BACK
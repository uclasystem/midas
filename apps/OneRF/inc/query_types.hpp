#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace onerf {
// util classes
struct DepQuery {
  std::vector<int64_t> size_vec;     // S
  std::vector<int8_t> cacheable_vec; // C
  std::vector<std::string> url_vec;  // U
};

using QueryLayer = std::map<std::string, DepQuery>;
using QueryLayerSlice = std::vector<QueryLayer>;

struct MemLimit {
  std::vector<int64_t> limits;
  std::vector<int64_t> mallocs;
};
using MemLimits = std::map<std::string, MemLimit>;
using PerControllerMemLimit = std::map<std::string, MemLimits>;

using HitStat = std::map<std::string, double>;
struct HitStatEntry {
  std::string raddr;
  HitStat hit_stat;
};

static inline std::string request_str(const QueryLayerSlice &request) {
  std::string str;
  str += std::to_string(request.size());
  str += ": [\n";
  for (auto &query : request) {
    str += "  {\n";
    for (auto &[k, v] : query) {
      str += "    " + k + " " + std::to_string(v.size_vec.size());
      str += "\n      ";
      for (auto s : v.size_vec) {
        str += std::to_string(s) + " ";
      }
      str += "\n      ";
      for (auto s : v.url_vec) {
        str += s + " ";
      }
      str += "\n      ";
      for (auto s : v.cacheable_vec) {
        str += std::to_string(s) + " ";
      }
      str += "\n";
    }
    str += "  }\n";
  }
  str += "]";
  return str;
}
} // namespace onerf
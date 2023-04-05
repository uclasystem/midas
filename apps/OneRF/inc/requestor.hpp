#pragma once

#include <string>
#include <vector>

#include "query_types.hpp"
#include "nuapp.hpp"

namespace onerf {
class Requestor {
public:
  Requestor(AppServer *nuapp,
            const std::string &trace_file_name = kTraceFileName);
  void InitTrace();
  void InitReqTimes();
  void Perf();

private:
  AppServer *nuapp_;
  std::vector<int64_t> req_times; // relative delays between two reqs, in us
  std::vector<QueryLayerSlice> requests_;
  std::string trace_file_;


  constexpr static char kTraceFileName[] = "tracegen/trace.json";
  constexpr static float kWarmupLambda = 200;
  constexpr static int64_t kWarmupReqs = 10000;
  constexpr static float kExperimentLambda = 400;
  constexpr static int64_t kExperimentReqs = 90000;
};
} // namespace onerf
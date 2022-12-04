#pragma once

namespace FeatExt {
class PerfAdapter {
public:
  void perf();

private:
  void gen_req();
  void serve_req();
};
} // namespace FeatExt
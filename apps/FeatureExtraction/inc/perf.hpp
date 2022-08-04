#pragma once


class PerfAdapter {
public:
  void perf();

private:
  void gen_req();
  void serve_req();
};
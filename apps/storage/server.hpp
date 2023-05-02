#pragma once

#include <iostream>
#include <fstream>
#include <memory>
#include <random>

// [midas]
#include "array.hpp"
#include "cache_manager.hpp"
#include "time.hpp"
#include "perf.hpp"
#include "zipf.hpp"

namespace storage {
constexpr static double kSkewness = 0.99;
constexpr static uint32_t kNumThds = 4;
constexpr static float kReadRatio = 0.7;
constexpr static float kWriteRatio = 1.0 - kReadRatio;

constexpr static uint64_t kPageSize = 4096; // 4KB
constexpr static uint64_t kNumPages = 4 * 1024 * 1024; // 4M pages = 16GB
constexpr static char kDiskPath[] = "fake_disk.bin";

using Page = char[kPageSize];

struct PgReq : midas::PerfRequest {
  int tid;
  enum { READ, WRITE } op;
  int pg_idx;
};

class Server : public midas::PerfAdapter  {
public:
  Server();
  ~Server();

  bool read(size_t pg_idx);
  bool write(size_t pg_idx);

  bool construct(size_t pg_idx);

  void warmup();
  std::unique_ptr<midas::PerfRequest> gen_req(int tid) override;
  bool serve_req(int tid, const midas::PerfRequest *img_req) override;

private:
  midas::CachePool *pool_;
  std::unique_ptr<midas::Array<Page>> page_cache_;
  std::mutex disk_mtx_;
  std::fstream disk_file_;

  std::unique_ptr<std::mt19937> gens[kNumThds];
  std::unique_ptr<midas::zipf_table_distribution<>> zipf_dist[kNumThds];
  std::unique_ptr<std::uniform_real_distribution<>> op_dist[kNumThds];
};
}
#pragma once

#include <atomic>
#include <cstdint>
#include <locale>
#include <map>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "backend.hpp"
#include "semaphore.hpp"

namespace onerf {
class FBack : public Backend {
public:
  struct Param {
    int32_t Hardness{0};
    int32_t IdCount{0};
    int32_t SizeLower{0};
    int32_t SizeUpper{0};
  };
  struct Matrix {
    float **data;
    int rows;
    int cols;

    Matrix();
    ~Matrix();
    void init(int rows_, int cols_);
    void free();
  };

  FBack(const struct Param &param, int max_conns);
  ~FBack() override;
  bool Request(const std::vector<std::string> &ids,
               std::map<std::string, Item> &res_map) override;
  bool GetIDs(const Param &param, std::vector<uint32_t> &data);
  void Report();

private:
  void calculate(int hardness);
  bool dot(const Matrix &x, const Matrix &y, Matrix &z);

  Param param_;
  Semaphore sem_;

  std::atomic_uint64_t sent_q_;
  std::atomic_uint64_t hardness_;
  std::atomic_uint64_t width_s_;
  std::atomic_int64_t conc_reqs_;

  bool terminated_;
  std::unique_ptr<std::thread> reporter_;
  std::random_device rd;
  std::mt19937 gen;
};
} // namespace onerf

#include <chrono>
#include <iostream>
#include <vector>

#include "fback.hpp"

namespace onerf {
FBack::FBack(const Param &param, int max_conn)
    : param_(param), sem_(max_conn), sent_q_(0), hardness_(0), width_s_(0),
      conc_reqs_(0), terminated_(false), gen(rd()) {
  reporter_ = std::make_unique<std::thread>([&] { Report(); });
}

FBack::~FBack() {
  terminated_ = true;
  reporter_->join();
}

bool FBack::Request(const std::vector<std::string> &ids,
                    std::map<std::string, Item> &res_map) {
  if (ids.empty()) {
    std::cerr << "Empty fback ids" << std::endl;
    return false;
  }

  sem_.get();
  auto param = param_;
  param.IdCount = ids.size();
  std::vector<uint32_t> reply;
  auto succ = GetIDs(param, reply);
  sem_.put();
  if (succ) {
    if (ids.size() == reply.size()) {
      for (int i = 0; i < ids.size(); i++) {
        res_map[ids[i]].reserve(reply[i]);
      }
    } else {
      std::cerr << "Incorrect reply count" << std::endl;
      return false;
    }
  }
  return true;
}

bool FBack::GetIDs(const Param &param, std::vector<uint32_t> &data) {
  conc_reqs_++;
  calculate(param.Hardness);
  auto rlen = param.IdCount;
  for (int i = 0; i < rlen; i++) {
    std::uniform_int_distribution<uint32_t> dist(0, param.SizeUpper -
                                                        param.SizeLower);
    uint32_t blocksize =
        1 << reinterpret_cast<uint32_t>(param.SizeLower + dist(gen));
    data.push_back(blocksize + 4);
  }

  sent_q_++;
  hardness_ += param.Hardness;
  width_s_ += rlen;
  conc_reqs_--;
  return true;
}

void FBack::Report() {
  while (!terminated_) {
    auto sentq = sent_q_.load();
    if (sentq > 0) {
      std::cout << sentq << " " << hardness_ << " " << width_s_ / sentq << " "
                << conc_reqs_ << std::endl;
    }
    sent_q_ = 0;
    hardness_ = 0;
    width_s_ = 0;
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));
  }
}

void FBack::calculate(int hardness) {
  Matrix x;
  Matrix y;
  x.init(hardness, hardness);
  y.init(hardness, hardness);
  Matrix z;
  dot(x, y, z);
  x.free();
  y.free();
  z.free();
}

bool FBack::dot(const Matrix &x, const Matrix &y, Matrix &z) {
  // this essentially restricts x and y to be square matrices
  if (x.width != y.height || x.height != y.width) {
    std::cerr << "Wrong matrix format!" << std::endl;
    return false;
  }
  z.init(x.height, y.width);
  for (int i = 0; i < x.height; i++) {
    for (int j = 0; j < y.height; j++) {
      z.data[i][j] = x.data[i][j] * y.data[j][i];
    }
  }

  return true;
}

FBack::Matrix::Matrix() : data(nullptr), height(0), width(0) {}

void FBack::Matrix::init(int height, int width) {
  data = new float *[height];
  for (int i = 0; i < height; i++) {
    data[i] = new float[width];
    for (int j = 0; j < width; j++) {
      data[i][j] = i + j; // random fill
    }
  }
}

void FBack::Matrix::free() {
  if (!data)
    return;
  for (int i = 0; i < height; i++)
    delete[] data[i];
  delete[] data;
  data = nullptr;
  height = width = 0;
}
} // namespace onerf

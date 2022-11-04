#pragma once

#include <cstddef>
#include <cstdint>

#include "utils.hpp"

namespace cachebank {

class ResilientFunc {
public:
  ResilientFunc(uint64_t stt_ip_, uint64_t end_ip_);
  void init(void *func_addr);
  bool contain(uint64_t fault_ip);
  bool omitted_frame_pointer;
  uint64_t fail_entry;

private:
  uint64_t stt_ip;
  uint64_t end_ip;
};

DECL_RESILIENT_FUNC(bool, rmemcpy, void *dst, const void *src, size_t len);
} // namespace cachebank

#include "impl/resilient_func.ipp"
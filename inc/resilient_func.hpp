#pragma once

#include <cstdint>
#include <cstddef>

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
bool FLATTEN rmemcpy(void *dst, const void *src,
                          size_t len) SOFT_RESILIENT;
void FLATTEN rmemcpy_end() SOFT_RESILIENT;
} // namespace cachebank

#include "impl/resilient_func.ipp"
#include <cassert>

namespace cachebank {
static inline bool match_fp_prologue(uint64_t func_stt) {
  /**
   * X86-64 function frame pointer prologue:
   *        0x55            push  %rbp
   *        ... (parameter passing, etc.)
   *        0x48 89 e5      mov   %rsp %rbp
   *        ...
   *        function frame pointer epilogue:
   *        0x5d            pop   %rbp
   *        0xc3            ret
   */
  // constexpr static uint32_t fp_prologue = 0xe5'89'48'55;
  constexpr static uint8_t fp_prologue = 0x55;
  uint8_t *code = reinterpret_cast<uint8_t *>(func_stt);
  return code[0] == fp_prologue;
}

inline ResilientFunc::ResilientFunc(uint64_t stt_ip_, uint64_t end_ip_)
    : stt_ip(stt_ip_), end_ip(end_ip_), fail_entry(0) {
  omitted_frame_pointer = !match_fp_prologue(stt_ip);
  if (omitted_frame_pointer) {
    constexpr static uint8_t int3 = 0xcc;
    constexpr static uint8_t retq = 0xc3;
    fail_entry = end_ip + 4; // we insert 4 int3 instructions in func_delimiter
    while (*(reinterpret_cast<uint8_t *>(fail_entry)) == int3)
      fail_entry++;
    assert(*(reinterpret_cast<uint8_t *>(fail_entry)) == retq);
  }
}

inline bool ResilientFunc::contain(uint64_t fault_ip) {
  return stt_ip < fault_ip && fault_ip < end_ip;
}

} // namespace cachebank
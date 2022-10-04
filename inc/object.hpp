#pragma once

#include <cstdint>

#include "transient_ptr.hpp"

namespace cachebank {

constexpr static uint32_t kSmallObjThreshold = 8 << 12; /* 32KB */

/** flags = 0, size = 0; rref = 0x1f1f1f1f1f1f */
constexpr static uint64_t kInvalidHdr = 0x0'000'1f1f1f1f1f1f;
constexpr static uint64_t kInvalidFlags = 0;

struct GenericObjectHdr {
  uint64_t flags;

  void set_invalid() noexcept;
  bool is_valid() const noexcept;
  bool is_small_obj() const noexcept;

private:
  constexpr static uint32_t kFlagShift =
      sizeof(flags) * 8; // start from the highest bit
  constexpr static decltype(flags) kPresentBit = kFlagShift - 1;
  constexpr static decltype(flags) kAccessedBit = kFlagShift - 2;
  constexpr static decltype(flags) kEvacuateBit = kFlagShift - 3;
  constexpr static decltype(flags) kSmallObjBit = kFlagShift - 4;
};

struct SmallObjectHdr {
  // Format:
  //  I) |P(1b)|A(1b)|E(1b)|S(1b)| Object Size(12b) | Reverse reference (48b) |
  //                   P: present bit.
  //                   A: accessed bits.
  //                   E: The pointed data is being evacuated.
  //                   S: small obj bit, meaning the object is a small obj (size
  //                     <= 8B * 2^12 == 32KB).
  //         Object Size: the size of the pointed object.
  //   Reverse reference: the only pointer referencing this object.
  void init(uint32_t size_, uint64_t rref = 0) noexcept;
  void free() noexcept;

  void set_invalid() noexcept;
  bool is_valid() noexcept;

  void set_size(uint32_t size_) noexcept;
  uint32_t get_size() const noexcept;

  void set_rref(uint64_t addr) noexcept;
  uint64_t get_rref() const noexcept;

  bool is_present() const noexcept;
  void set_present() noexcept;
  void clr_present() noexcept;
  bool is_accessed() const noexcept;
  void set_accessed() noexcept;
  void clr_accessed() noexcept;
  bool is_evacuate() const noexcept;
  void set_evacuate() noexcept;
  void clr_evacuate() noexcept;

private:
  uint64_t rref : 48; // reverse reference to the single to-be-updated pointer
  uint16_t size : 12; // object size / 8 (8 Bytes is the base unit)
  uint8_t flags : 4;

  void _small_obj() noexcept;
  constexpr static decltype(flags) kFlagShift = 4;
  constexpr static decltype(flags) kPresentBit = kFlagShift - 1;
  constexpr static decltype(flags) kAccessedBit = kFlagShift - 2;
  constexpr static decltype(flags) kEvacuateBit = kFlagShift - 3;
  constexpr static decltype(flags) kSmallObjBit = kFlagShift - 4;
};

static_assert(sizeof(SmallObjectHdr) <= 16,
              "SmallObjHdr is not correctly aligned!");

struct LargeObjectHdr {
  // Format:
  //  I) | P(1b) | A(1b) | E(1b) | S(1b) | C(1b) | 000(27b) | Object Size(32b) |
  // II) |             0...0(16b)             |    Reverse reference (48b)     |
  //                   P: present bit.
  //                   A: accessed bits.
  //                   E: evacuate bit, meaning the data is being evacuated.
  //                   S: small obj bit, meaning the object is a small obj
  //                     (size <= 8B * 2^12 == 32KB).
  //                   C: continue bit, meaning the objct is a large obj and
  //                      the current chunk is a continued chunk.
  //         Object Size: the size of the pointed object.
  //   Reverse reference: the only pointer referencing this object.

  void init(uint32_t size_, uint64_t rref = 0) noexcept;
  void free() noexcept;

  void set_invalid() noexcept;
  bool is_valid() noexcept;

  void set_size(uint32_t size_) noexcept;
  uint32_t get_size() const noexcept;

  void set_rref(uint64_t addr) noexcept;
  uint64_t get_rref() const noexcept;

  bool is_present() const noexcept;
  void set_present() noexcept;
  void clr_present() noexcept;
  bool is_accessed() const noexcept;
  void set_accessed() noexcept;
  void clr_accessed() noexcept;
  bool is_evacuate() const noexcept;
  void set_evacuate() noexcept;
  void clr_evacuate() noexcept;
  bool is_continue() const noexcept;
  void set_continue() noexcept;
  void clr_continue() noexcept;

private:
  uint32_t flags;
  uint32_t size;
  uint64_t rref; // reverse reference

  void _large_obj() noexcept;
  constexpr static uint32_t kFlagShift =
      sizeof(flags) * 8; // start from the highest bit
  constexpr static decltype(flags) kPresentBit = kFlagShift - 1;
  constexpr static decltype(flags) kAccessedBit = kFlagShift - 2;
  constexpr static decltype(flags) kEvacuateBit = kFlagShift - 3;
  constexpr static decltype(flags) kSmallObjBit = kFlagShift - 4;
  constexpr static decltype(flags) kContinueBit = kFlagShift - 5;
};

static_assert(sizeof(LargeObjectHdr) <= 16,
              "LargeObjHdr is not correctly aligned!");

struct ObjectPtr {
public:
  bool set(uint64_t stt_addr, size_t data_size);
  bool init_from_soft(uint64_t soft_addr);
  bool free() noexcept;

  /** Header related */
  static size_t total_size(size_t data_size) noexcept;
  size_t total_size() const noexcept;
  size_t hdr_size() const noexcept;
  size_t data_size() const noexcept;

  bool set_invalid() noexcept;
  bool is_valid() noexcept;

  bool set_rref(uint64_t addr) noexcept;
  uint64_t get_rref() noexcept;

  bool is_small_obj() const noexcept;

  bool is_present() noexcept;
  bool set_present() noexcept;
  bool clr_present() noexcept;
  bool is_accessed() noexcept;
  bool set_accessed() noexcept;
  bool clr_accessed() noexcept;
  bool is_evacuate() noexcept;
  bool set_evacuate() noexcept;
  bool clr_evacuate() noexcept;
  bool is_continue() noexcept;
  bool set_continue() noexcept;
  bool clr_continue() noexcept;

  /** Data related */
  bool cmpxchg(int64_t offset, uint64_t oldval, uint64_t newval);

  bool copy_from(const void *src, size_t len, int64_t offset = 0);
  bool copy_to(void *dst, size_t len, int64_t offset = 0);
  bool copy_from(ObjectPtr &src, size_t len, int64_t from_offset = 0,
                 int64_t to_offset = 0);
  bool copy_to(ObjectPtr &dst, size_t len, int64_t from_offset = 0,
               int64_t to_offset = 0);

private:
  size_t size_;
  TransientPtr obj_;
};

} // namespace cachebank

#include "impl/object.ipp"
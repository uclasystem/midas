#pragma once

#include <cstdint>

namespace cachebank {

constexpr static uint64_t kInvalidHdr = 0x1f1f1f1f'1f1f1f1f;
constexpr static uint64_t kInvalidFlags = 0;

struct GenericObjecthdr {
  uint64_t flags;

  bool is_valid() const noexcept;
  bool is_small_obj() const noexcept;

private:
  constexpr static uint32_t kFlagShift =
      sizeof(flags) - 4; // only use the highest 4 bits
  constexpr static uint32_t kPresentBit = 0 + kFlagShift;
  constexpr static uint32_t kAccessedBit = 1 + kFlagShift;
  constexpr static uint32_t kEvacuateBit = 2 + kFlagShift;
  constexpr static uint16_t kSmallObjBit = 3 + kFlagShift;
};

struct SmallObjectHdr {
  // Format:
  //  I) |!S(1b)|E(1b)|A(1b)|P(1b)| Object Size(12b) | Reverse reference (48b) |
  //                   P: present bit.
  //                   A: accessed bits.
  //                   E: The pointed data is being evacuated.
  //                   S: small obj bit, meaning the object is a small obj (size
  //                     <= 8B * 2^12 == 32KB)
  //         Object Size: the size of the pointed object.
  //   Reverse reference: the only pointer referencing this object
  uint16_t flags : 4;
  uint16_t size : 12;
  uint64_t rref : 48; // reverse reference to the single to-be-updated pointer

  void init(uint32_t size_) noexcept;
  void free() noexcept;

  void set_size(uint32_t size_) noexcept;
  uint16_t get_size() const noexcept;

  void set_rref(uint64_t addr) noexcept;
  uint64_t get_rref() const noexcept;

  void set_invalid() noexcept;
  void clr_invalid() noexcept;

  void set_present() noexcept;
  void clr_present() noexcept;
  void set_accessed() noexcept;
  void clr_accessed() noexcept;
  void set_evacuate() noexcept;
  void clr_evacuate() noexcept;

private:
  void _small_obj() noexcept;
  constexpr static uint8_t kPresentBit = 0;
  constexpr static uint8_t kAccessedBit = 1;
  constexpr static uint8_t kEvacuateBit = 2;
  constexpr static uint8_t kSmallObjBit = 3;
};

static_assert(sizeof(SmallObjectHdr) <= 16,
              "SmallObjHdr is not correctly aligned!");

struct LargeObjectHdr {
  // Format:
  //  I) | !S(1b) | E(1b) | A(1b) | P(1b) | 0...0(28b) |   Object Size(32b)    |
  // II) |             0...0(16b)             |    Reverse reference (48b)     |
  //                   P: present bit.
  //                   A: accessed bits.
  //                   E: The pointed data is being evacuated.
  //                   S: small obj bit, meaning the object is a small obj
  //                     (size <= 8B * 2^12 == 32KB)
  //         Object Size: the size of the pointed object.
  //   Reverse reference: the only pointer referencing this object

  uint32_t flags;
  uint32_t size;
  uint64_t rref; // reverse reference

  void init(uint32_t size_) noexcept;
  void free() noexcept;

  void set_size(uint32_t size_) noexcept;
  uint32_t get_size() const noexcept;

  void set_rref(uint64_t addr) noexcept;
  uint64_t get_rref() const noexcept;

  void set_invalid() noexcept;
  void clr_invalid() noexcept;

  void set_present() noexcept;
  void clr_present() noexcept;
  void set_accessed() noexcept;
  void clr_accessed() noexcept;
  void set_evacuate() noexcept;
  void clr_evacuate() noexcept;

private:
  void _large_obj() noexcept;
  constexpr static uint32_t kFlagShift =
      sizeof(flags) - 4; // only use the highest 4 bits
  constexpr static uint32_t kPresentBit = 0 + kFlagShift;
  constexpr static uint32_t kAccessedBit = 1 + kFlagShift;
  constexpr static uint32_t kEvacuateBit = 2 + kFlagShift;
  constexpr static uint16_t kSmallObjBit = 3 + kFlagShift;
};

static_assert(sizeof(LargeObjectHdr) <= 16,
              "LargeObjHdr is not correctly aligned!");
} // namespace cachebank

#include "impl/object.ipp"
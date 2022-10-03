#include <cstddef>
#include <iostream>

#include "object.hpp"

int main() {
  void *ref = nullptr;
  std::cout << "ref pointer addr: " << &ref << std::endl;

  cachebank::SmallObjectHdr small_obj;
  small_obj.init(32, reinterpret_cast<uint64_t>(&ref));
  std::cout << std::hex << small_obj.get_size() << " " << small_obj.get_rref()
            << std::endl;
  std::cout << std::hex << *(reinterpret_cast<uint64_t *>(&small_obj))
            << std::endl;
  small_obj.init(32 * 1024 - 8, reinterpret_cast<uint64_t>(&ref));
  std::cout << small_obj.get_size() << " " << std::hex << small_obj.get_rref()
            << std::endl;
  std::cout << std::hex << *(reinterpret_cast<uint64_t *>(&small_obj))
            << std::endl;
  /* size is too large, failed */
  // small_obj.init(32 * 1024 - 7, reinterpret_cast<uint64_t>(&ref)); // failed
  // std::cout << small_obj.get_size() << " " << std::hex << small_obj.get_rref()
  //           << std::endl;
  // std::cout << std::hex << *(reinterpret_cast<uint64_t *>(&small_obj))
  //           << std::endl;

  cachebank::LargeObjectHdr large_obj;
  large_obj.init(32 * 1024 - 7, reinterpret_cast<uint64_t>(&ref));
  std::cout << large_obj.get_size() << " " << std::hex << large_obj.get_rref()
            << std::endl;
  std::cout << std::hex << *(reinterpret_cast<uint32_t *>(&large_obj)) << " "
            << *(reinterpret_cast<uint32_t *>(&large_obj) + 1) << " "
            << *(reinterpret_cast<uint64_t *>(&large_obj) + 1) << std::endl;

  return 0;
}
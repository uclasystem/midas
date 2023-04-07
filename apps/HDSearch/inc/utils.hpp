#pragma once

#include <cstring>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <openssl/md5.h>
#include <string_view>

#include "constants.hpp"

namespace hdsearch {

struct MD5Key {
  char data[kMD5Len];

  const std::string to_string() {
    char str[kMD5Len + 1];
    std::memcpy(str, data, kMD5Len);
    str[kMD5Len] = '\0';
    return str;
  }
};

struct Feature {
  float data[kFeatDim];
};
static_assert(sizeof(Feature) == sizeof(float) * kFeatDim,
              "Feature struct size incorrect");

struct FeatReq {
  int tid;
  int rid;
  std::string filename;
  Feature *feat;
  uint64_t start_us;
};

struct Trace {
  uint64_t absl_start_us;
  uint64_t start_us;
  uint64_t duration;
};

static inline void md5_from_file(MD5Key &md5_result,
                                 const std::string &filename) {
  std::ifstream file(filename, std::ifstream::binary);
  MD5_CTX md5Context;
  MD5_Init(&md5Context);
  char buf[1024 * 16];
  while (file.good()) {
    file.read(buf, sizeof(buf));
    MD5_Update(&md5Context, buf, file.gcount());
  }
  unsigned char result[MD5_DIGEST_LENGTH];
  MD5_Final(result, &md5Context);

  std::stringstream md5str_stream;
  md5str_stream << std::hex << std::uppercase << std::setfill('0');
  for (const auto &byte : result)
    md5str_stream << std::setw(2) << (int)byte;
  md5str_stream << "\0";
  std::memcpy(md5_result.data, md5str_stream.str().c_str(), kMD5Len);
}

} // namespace hdsearch

namespace std {
template <> struct hash<hdsearch::MD5Key> {
  size_t operator()(const hdsearch::MD5Key &k) const {
    return std::hash<std::string_view>()(
        std::string_view(k.data, hdsearch::kMD5Len));
  }
};

template <> struct equal_to<hdsearch::MD5Key> {
  size_t operator()(const hdsearch::MD5Key &k1,
                    const hdsearch::MD5Key &k2) const {
    return std::memcmp(k1.data, k2.data, hdsearch::kMD5Len) == 0;
  }
};
} // namespace std

#pragma once

#include <cstring>
#include <fstream>
#include <iomanip>
#include <memory>
#include <openssl/md5.h>
#include <string>
#include <string_view>
#include <sw/redis++/cxx_utils.h>
#include <sw/redis++/redis++.h>

#include "constants.hpp"

namespace FeatExt {
inline std::string ExecuteShellCommand(const std::string cmd) {
  const int BUF_SIZE = 1024;
  char buffer[BUF_SIZE];
  std::string result = "";
  std::shared_ptr<FILE> pipe(popen(cmd.c_str(), "r"), pclose);
  if (!pipe)
    throw std::runtime_error("popen() failed!");
  while (!feof(pipe.get())) {
    if (fgets(buffer, BUF_SIZE, pipe.get()) != NULL)
      result += buffer;
  }
  return result;
}

inline const std::string md5_from_file(const std::string &filename) {
  // std::string fullname = std::string("../validation/") + filename;
  std::string fullname = std::string("") + filename;
  std::ifstream file(fullname, std::ifstream::binary);
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
  return md5str_stream.str();
}

struct MD5Key {
  char data[kMD5Len];

  void from_string(const std::string &str) {
    std::memcpy(data, str.c_str(), kMD5Len);
  }
  const std::string to_string() {
    char str[kMD5Len + 1];
    std::memcpy(str, data, kMD5Len);
    str[kMD5Len] = '\0';
    return str;
  }
};
static_assert(sizeof(MD5Key) == kMD5Len, "MD5Key size incorrect");

struct Feature {
  float data[kFeatDim];

  sw::redis::StringView to_string_view() {
    return sw::redis::StringView(reinterpret_cast<const char *>(data),
                                 sizeof(Feature));
  }
};
static_assert(sizeof(Feature) == sizeof(float) * kFeatDim,
              "Feature struct size incorrect");
} // namespace FeatExt
#pragma once

#include <iomanip>
#include <openssl/md5.h>
#include <fstream>

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

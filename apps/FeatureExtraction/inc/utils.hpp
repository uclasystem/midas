#pragma once

#include <memory>
#include <string>

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

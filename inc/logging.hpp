#pragma once

#include <chrono>
#include <ctime>
#include <iostream>
#include <iomanip>
#include <sstream>

namespace cachebank {

enum LogVerbosity {
  kError = 0,
  kWarning = 1,
  kInfo = 2,  // information may interests users.
  kDebug = 3, // information only interesting to developers.
  kAll = 4,   // log all information
};

constexpr LogVerbosity kGlobalVerbose = kAll;
constexpr bool kLogFlagTime = false;
constexpr bool kLogFlagLoc = false;

class Logger {
public:
  Logger(const std::string &file, const std::string &func, int line, LogVerbosity verbose,
         const std::string &verbose_str) noexcept
      : _verbose(verbose) {
    if (_verbose > kGlobalVerbose)
      return;
    if (kLogFlagTime) {
      auto now = std::chrono::system_clock::to_time_t(
          std::chrono::system_clock::now());
      std::cerr << "[" << std::put_time(std::localtime(&now), "%c") << "]";
    }
    std::cerr << "[" << verbose_str << "]";
    if (kLogFlagLoc) {
      std::cerr << "(" << file << "/" << func << ":" << line << "):";
    }
    std::cerr << " ";
  }

  template <class T> Logger &operator<<(const T &v) {
    if (_verbose > kGlobalVerbose)
      return *this;
    std::cerr << v;
    return *this;
  }

  ~Logger() { std::cerr << std::endl; }

private:
  LogVerbosity _verbose;
};

#define LOG(verbose) Logger(__FILE__, __func__, __LINE__, (verbose), #verbose)

} // namespace cachebank
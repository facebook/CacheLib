#pragma once
#include <chrono>
#include <ctime>

namespace facebook {
namespace cachelib {
namespace util {

// ::time is the fastest for getting the second granularity steady clock
// through the vdso. This is faster than std::chrono::steady_clock::now and
// counting it as seconds since epoch.
inline uint32_t getCurrentTimeSec() {
  // time in seconds since epoch will fit in 32 bit. We use this primarily for
  // storing in cache.
  return static_cast<uint32_t>(std::time(nullptr));
}

// For nano second granularity, std::chrono::steady_clock seems to do a fine
// job.
inline uint64_t getCurrentTimeMs() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(ret).count();
}

inline uint64_t getCurrentTimeNs() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(ret).count();
}

inline uint32_t getSteadyCurrentTimeSec() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::seconds>(ret).count();
}
} // namespace util
} // namespace cachelib
} // namespace facebook

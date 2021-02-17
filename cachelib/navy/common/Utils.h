#pragma once

#include <folly/lang/Bits.h>
#include <folly/logging/xlog.h>

#include <cassert>
#include <chrono>
#include <memory>

#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
inline std::chrono::nanoseconds getSteadyClock() {
  auto dur = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(dur);
}

inline std::chrono::seconds getSteadyClockSeconds() {
  auto dur = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::seconds>(dur);
}

inline std::chrono::microseconds toMicros(std::chrono::nanoseconds t) {
  return std::chrono::duration_cast<std::chrono::microseconds>(t);
}

inline size_t powTwoAlign(size_t size, size_t boundary) {
  XDCHECK(folly::isPowTwo(boundary)) << boundary;
  return (size + (boundary - 1)) & ~(boundary - 1);
}

// Estimates actual slot size that malloc(@size) actually allocates. This
// better estimates actual memory used.
inline size_t mallocSlotSize(size_t bytes) {
  if (bytes <= 8) {
    return 8;
  }
  if (bytes <= 128) {
    return powTwoAlign(bytes, 16);
  }
  if (bytes <= 512) {
    return powTwoAlign(bytes, 64);
  }
  if (bytes <= 4096) {
    return powTwoAlign(bytes, 256);
  }
  // Accurate till 4MB
  return powTwoAlign(bytes, 4096);
}

inline const uint8_t* bytePtr(const void* ptr) {
  return reinterpret_cast<const uint8_t*>(ptr);
}

template <typename T>
inline bool between(T x, NoDeduce<T> a, NoDeduce<T> b) {
  return a <= x && x <= b;
}

template <typename T>
inline bool betweenStrict(T x, NoDeduce<T> a, NoDeduce<T> b) {
  return a < x && x < b;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

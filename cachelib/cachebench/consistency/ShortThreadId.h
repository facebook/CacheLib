#pragma once

#include <folly/SharedMutex.h>

#include <cstdint>
#include <thread>
#include <unordered_map>

namespace facebook {
namespace cachelib {
namespace cachebench {
using ShortThreadId = uint16_t;

// Assigns short thread ids to std thread ids. Keeps a map internally under
// the shared lock - expect infrequent updates.
class ShortThreadIdMap {
 public:
  ShortThreadIdMap() = default;
  ShortThreadId getShort(std::thread::id tid);

 private:
  mutable folly::SharedMutex mutex_;
  std::unordered_map<std::thread::id, ShortThreadId> tids_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#pragma once

#include <folly/Range.h>

#include <cstdint>
#include <functional>
#include <ostream>

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Generic operation status
enum class Status {
  Ok,

  // Entry not found
  NotFound,

  // Operations were rejected (queue full, admission policy, etc.)
  Rejected,

  // Resource is temporary busy
  Retry,

  // Device IO error or out of memory
  DeviceError,

  // Internal invariant broken. Consistency may be violated.
  BadState,
};

enum class DestructorEvent {
  Recycled,
  Removed,
};

// @key and @value are valid only during this callback invocation
using DestructorCallback = std::function<void(
    BufferView key, BufferView value, DestructorEvent event)>;

// Export counters by visiting them in the object hierarchy
using CounterVisitor =
    std::function<void(folly::StringPiece name, double count)>;

constexpr uint32_t kMaxKeySize{255};

const char* toString(Status status);
const char* toString(DestructorEvent e);

inline std::ostream& operator<<(std::ostream& os, Status status) {
  return os << "Status::" << toString(status);
}

inline std::ostream& operator<<(std::ostream& os, DestructorEvent e) {
  return os << "DestructorEvent::" << toString(e);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

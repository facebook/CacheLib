/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/Range.h>

#include <cstdint>
#include <functional>
#include <ostream>

#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"

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
  // space is recycled (item evicted)
  Recycled,
  // item is removed from NVM
  Removed,
  // item already in the queue but failed to put into NVM
  PutFailed,
};

// @key and @value are valid only during this callback invocation
using DestructorCallback =
    std::function<void(HashedKey hk, BufferView value, DestructorEvent event)>;

// Checking NvmItem expired
using ExpiredCheck = std::function<bool(BufferView value)>;

// Get CounterVisitor into navy namespace.
using CounterVisitor = util::CounterVisitor;

constexpr uint32_t kMaxKeySize{255};

// Convert status to string message. Return "Unknown" if invalid status.
const char* toString(Status status);

// Convert event to string message. Return "Unknown" if invalid event.
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

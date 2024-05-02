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

#include <folly/logging/xlog.h>
#include <folly/memory/Malloc.h>

namespace facebook {
namespace cachelib {
namespace objcache2 {

// Tracks the memory usage in a local thread.
// Use this class to calculate the object size when enabling size-awareness.
// Example:
//      ThreadMemoryTracker tMemTracker;
//      auto beforeMemUsage = tMemTracker.getMemUsageBytes();
//      ... create the object
//      auto afterMemUsage = tMemTracker.getMemUsageBytes();
//      ...
//      auto objectSize = LIKELY(afterMemUsage > beforeMemUsage) ?
//                        (afterMemUsage - beforeMemUsage) : 0;
class ThreadMemoryTracker {
 public:
  ThreadMemoryTracker() {
    if (folly::usingJEMalloc()) {
      size_t size = sizeof(uint64_t*);
      mallctl("thread.allocatedp", &allocPtr_, &size, nullptr, 0);
      mallctl("thread.deallocatedp", &deallocPtr_, &size, nullptr, 0);
    }
  }

  int64_t getMemUsageBytes() {
    if (!allocPtr_ || !deallocPtr_) {
      return 0;
    }
    return *allocPtr_ - *deallocPtr_;
  }

 private:
  // pointer of the total number of bytes ever allocated by the calling thread
  uint64_t* allocPtr_{nullptr};
  // pointer of the total number of bytes ever deallocated by the calling thread
  uint64_t* deallocPtr_{nullptr};
};
} // namespace objcache2
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/cachebench/consistency/ShortThreadId.h"

#include <limits>
#include <mutex>
#include <shared_mutex>

namespace facebook {
namespace cachelib {
namespace cachebench {
ShortThreadId ShortThreadIdMap::getShort(std::thread::id tid) {
  {
    std::shared_lock<folly::SharedMutex> lock{mutex_};
    auto iter = tids_.find(tid);
    if (iter != tids_.end()) {
      return iter->second;
    }
  }

  // Now we have to insert a new TID. Take writer lock. Remember to check
  // again if present!
  std::lock_guard<folly::SharedMutex> lock{mutex_};
  auto iter = tids_.find(tid);
  if (iter != tids_.end()) {
    return iter->second;
  }
  auto size = tids_.size();
  if (size > std::numeric_limits<ShortThreadId>::max()) {
    throw std::out_of_range("too many threads");
  }
  // "first" is an iterator pointing to the inserted key/value pair
  return tids_.emplace(tid, static_cast<ShortThreadId>(size)).first->second;
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

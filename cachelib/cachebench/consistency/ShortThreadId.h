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

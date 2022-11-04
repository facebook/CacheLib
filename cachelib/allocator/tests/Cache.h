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

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {
// Vanilla version of Cache supplying necessary components to initialize
// CacheAllocatorConfig
struct Cache {
  using AccessType = LruCacheTrait::AccessType;
  using AccessTypeLocks = LruCacheTrait::AccessTypeLocks;
  struct AccessConfig {};
  struct ChainedItemMovingSync {};
  struct RemoveCb {};
  struct NvmCacheFilterCb {};
  struct NvmCacheT {
    struct EncodeCB {};
    struct DecodeCB {};
    struct DeviceEncryptor {};
    struct Config {};
  };
  struct MoveCb {};
  struct Key {};
  struct EventTracker {};
  using MMType = MM2Q;
  struct Item {
    using Key = folly::StringPiece;

    explicit Item(const std::string& key) : key_(key) {}
    Item(const std::string& key, const uint64_t ttl) : key_(key), ttl_(ttl) {}

    Key getKey() const { return key_; }

    std::chrono::seconds getConfiguredTTL() const {
      return std::chrono::seconds(ttl_);
    }

    std::string key_;
    uint64_t ttl_{0};
  };

  using ChainedItemIter = std::vector<Item>::iterator;
};

} // namespace tests
} // namespace cachelib
} // namespace facebook

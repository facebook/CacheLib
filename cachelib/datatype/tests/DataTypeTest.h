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

#include <memory>

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook {
namespace cachelib {
namespace tests {
struct DataTypeTest {
  static const std::string kDefaultPool;

  // Creates a bare minimum Cache for test cases
  // Default Pool ID is 0
  template <typename AllocatorT>
  static std::unique_ptr<AllocatorT> createCache() {
    typename AllocatorT::Config config;
    config.configureChainedItems();
    config.setCacheSize(100 * Slab::kSize);
    auto cache = std::make_unique<AllocatorT>(config);
    const size_t numBytes = cache->getCacheMemoryStats().ramCacheSize;
    cache->addPool(kDefaultPool, numBytes);
    return cache;
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

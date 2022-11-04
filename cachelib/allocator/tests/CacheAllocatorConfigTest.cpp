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

#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/MemoryTierCacheConfig.h"
#include "cachelib/allocator/tests/TestBase.h"

namespace facebook {
namespace cachelib {

namespace tests {

using AllocatorT = LruAllocator;
using MemoryTierConfigs = CacheAllocatorConfig<AllocatorT>::MemoryTierConfigs;

size_t defaultTotalSize = 1 * 1024LL * 1024LL * 1024LL;

class CacheAllocatorConfigTest : public testing::Test {};

MemoryTierConfigs generateTierConfigs(size_t numTiers,
                                      MemoryTierCacheConfig& config) {
  return MemoryTierConfigs(numTiers, config);
}

TEST_F(CacheAllocatorConfigTest, MultipleTier0Config) {
  AllocatorT::Config config;
  // Throws if vector of tier configs is emptry
  EXPECT_THROW(config.configureMemoryTiers(MemoryTierConfigs()),
               std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, MultipleTier1Config) {
  AllocatorT::Config config;
  // Accepts single-tier configuration
  config.setCacheSize(defaultTotalSize)
      .configureMemoryTiers({MemoryTierCacheConfig::fromShm().setRatio(1)});
  config.validateMemoryTiers();
}

TEST_F(CacheAllocatorConfigTest, InvalidTierRatios) {
  AllocatorT::Config config;
  EXPECT_THROW(config.configureMemoryTiers(generateTierConfigs(
                   config.kMaxCacheMemoryTiers + 1,
                   MemoryTierCacheConfig::fromShm().setRatio(0))),
               std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, TotalCacheSizeLessThanRatios) {
  AllocatorT::Config config;
  // Throws if total cache size is set to 0
  config.setCacheSize(defaultTotalSize)
      .configureMemoryTiers(
          {MemoryTierCacheConfig::fromShm().setRatio(defaultTotalSize + 1)});
  EXPECT_THROW(config.validate(), std::invalid_argument);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

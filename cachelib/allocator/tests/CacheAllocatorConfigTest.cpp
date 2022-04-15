/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

// TODO: Increase this constant when multi-tier configs are fully enabled
size_t maxNumTiers = 1;
size_t defaultTotalSize = 2 * Slab::kSize * maxNumTiers;

class CacheAllocatorConfigTest : public testing::Test {};

MemoryTierConfigs generateTierConfigs(size_t numTiers,
                                      MemoryTierCacheConfig& config) {
  return MemoryTierConfigs(numTiers, config);
}

size_t sumTierSizes(const MemoryTierConfigs& configs) {
  size_t sumSizes = 0;
  for (const auto& config : configs) {
    sumSizes += config.getSize();
  }
  return sumSizes;
}

TEST_F(CacheAllocatorConfigTest, MultipleTier0Config) {
  AllocatorT::Config config1, config2;
  // Throws if vector of tier configs is emptry
  EXPECT_THROW(config1.configureMemoryTiers(MemoryTierConfigs()),
               std::invalid_argument);
  EXPECT_THROW(
      config2.configureMemoryTiers(defaultTotalSize, MemoryTierConfigs()),
      std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, MultipleTier1Config) {
  AllocatorT::Config config1, config2, config3;
  // Accepts single-tier configuration
  config1.setCacheSize(defaultTotalSize)
      .configureMemoryTiers(
          {MemoryTierCacheConfig::fromShm().setSize(defaultTotalSize)});
  config2.setCacheSize(0).configureMemoryTiers(
      {MemoryTierCacheConfig::fromShm().setSize(defaultTotalSize)});
  config3.configureMemoryTiers(defaultTotalSize,
                               {MemoryTierCacheConfig::fromShm().setRatio(1)});
}

TEST_F(CacheAllocatorConfigTest, MultipleTier2Config) {
  AllocatorT::Config config1, config2;
  // Throws if vector of tier configs > 1
  EXPECT_THROW(config1.configureMemoryTiers(generateTierConfigs(
                   maxNumTiers + 1,
                   MemoryTierCacheConfig::fromShm().setSize(defaultTotalSize))),
               std::invalid_argument);
  EXPECT_THROW(
      config2.configureMemoryTiers(
          defaultTotalSize,
          generateTierConfigs(maxNumTiers + 1,
                              MemoryTierCacheConfig::fromShm().setRatio(1))),
      std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, TotalCacheSizeSet0) {
  AllocatorT::Config config1, config2;
  // If total cache size is set to zero and each individual tier specifies its
  // size then total cache size is set to the sum of tier sizes
  MemoryTierConfigs tierConfigs = {
      MemoryTierCacheConfig::fromShm().setSize(defaultTotalSize)};
  config1.setCacheSize(0).configureMemoryTiers(tierConfigs);
  ASSERT_EQ(config1.getCacheSize(),
            sumTierSizes(config1.getMemoryTierConfigs()));

  // Throws if total cache size is set to 0 and ratios are used to specify sizes
  EXPECT_THROW(config2.configureMemoryTiers(
                   0, {MemoryTierCacheConfig::fromShm().setRatio(1)}),
               std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, TotalCacheSizeLessThanRatios) {
  AllocatorT::Config config1, config2;
  // Throws if total cache size is set to 0
  EXPECT_THROW(
      config1.setCacheSize(defaultTotalSize)
          .configureMemoryTiers({MemoryTierCacheConfig::fromShm().setRatio(
              defaultTotalSize + 1)}),
      std::invalid_argument);
  EXPECT_THROW(
      config2.configureMemoryTiers(
          defaultTotalSize,
          {MemoryTierCacheConfig::fromShm().setRatio(defaultTotalSize + 1)}),
      std::invalid_argument);
}

TEST_F(CacheAllocatorConfigTest, SumTierSizes) {
  AllocatorT::Config config1, config2;
  MemoryTierConfigs sizeConfigs = generateTierConfigs(
      maxNumTiers,
      MemoryTierCacheConfig::fromShm().setSize(defaultTotalSize / maxNumTiers));
  MemoryTierConfigs ratioConfigs = generateTierConfigs(
      maxNumTiers, MemoryTierCacheConfig::fromShm().setRatio(1));

  config1.setCacheSize(defaultTotalSize).configureMemoryTiers(sizeConfigs);
  ASSERT_EQ(config1.getCacheSize(),
            sumTierSizes(config1.getMemoryTierConfigs()));

  config2.setCacheSize(defaultTotalSize).configureMemoryTiers(ratioConfigs);
  ASSERT_EQ(config2.getCacheSize(),
            sumTierSizes(config2.getMemoryTierConfigs()));
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

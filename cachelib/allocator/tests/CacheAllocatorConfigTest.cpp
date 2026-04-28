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
#include "cachelib/common/EventSink.h"
#include "cachelib/common/EventTracker.h"

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

TEST_F(CacheAllocatorConfigTest, SerializeEvictionPolicyLru) {
  LruAllocator::Config config;
  auto serialized = config.serialize();
  EXPECT_EQ(serialized["evictionPolicy"], "MMLru");
}

TEST_F(CacheAllocatorConfigTest, SerializeEvictionPolicy2Q) {
  Lru2QAllocator::Config config;
  auto serialized = config.serialize();
  EXPECT_EQ(serialized["evictionPolicy"], "MM2Q");
}

TEST_F(CacheAllocatorConfigTest, SerializeEvictionPolicyTinyLFU) {
  TinyLFUAllocator::Config config;
  auto serialized = config.serialize();
  EXPECT_EQ(serialized["evictionPolicy"], "MMTinyLFU");
}

TEST_F(CacheAllocatorConfigTest, SerializeEvictionPolicyWTinyLFU) {
  WTinyLFUAllocator::Config config;
  auto serialized = config.serialize();
  EXPECT_EQ(serialized["evictionPolicy"], "MMWTinyLFU");
}

TEST_F(CacheAllocatorConfigTest, SetEventTrackerConfigFactory) {
  AllocatorT::Config config;
  int factoryCallCount = 0;
  config.setEventTrackerConfigFactory([&factoryCallCount]() {
    ++factoryCallCount;
    EventTracker::Config trackerConfig;
    trackerConfig.queueSize = 100;
    trackerConfig.eventSink = std::make_unique<InMemoryEventSink>();
    trackerConfig.sampler = std::make_unique<FurcHashSampler>(1);
    return trackerConfig;
  });
  EXPECT_TRUE(config.eventTrackerConfigFactory != nullptr);
  EXPECT_EQ(factoryCallCount, 0);

  // Invoke the factory and verify it produces a valid config.
  auto trackerConfig = config.eventTrackerConfigFactory();
  EXPECT_EQ(factoryCallCount, 1);
  EXPECT_EQ(trackerConfig.queueSize, 100);
  EXPECT_NE(trackerConfig.eventSink, nullptr);
  EXPECT_NE(trackerConfig.sampler, nullptr);
}

TEST_F(CacheAllocatorConfigTest, EventTrackerConfigFactoryProducesFreshConfig) {
  AllocatorT::Config config;
  config.setEventTrackerConfigFactory([]() {
    EventTracker::Config trackerConfig;
    trackerConfig.queueSize = 50;
    trackerConfig.eventSink = std::make_unique<InMemoryEventSink>();
    return trackerConfig;
  });

  // Call factory twice and verify each produces independent, valid configs.
  auto config1 = config.eventTrackerConfigFactory();
  auto config2 = config.eventTrackerConfigFactory();
  EXPECT_NE(config1.eventSink, nullptr);
  EXPECT_NE(config2.eventSink, nullptr);
  // The two sinks must be distinct objects.
  EXPECT_NE(config1.eventSink.get(), config2.eventSink.get());
}

TEST_F(CacheAllocatorConfigTest, SerializeEventTrackerConfigFactory) {
  AllocatorT::Config config;
  // Before setting, serialization should show "empty".
  auto serialized = config.serialize();
  EXPECT_EQ(serialized["eventTrackerConfigFactory"], "empty");

  config.setEventTrackerConfigFactory([]() {
    EventTracker::Config trackerConfig;
    trackerConfig.queueSize = 10;
    trackerConfig.eventSink = std::make_unique<InMemoryEventSink>();
    return trackerConfig;
  });
  serialized = config.serialize();
  EXPECT_EQ(serialized["eventTrackerConfigFactory"], "set");
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

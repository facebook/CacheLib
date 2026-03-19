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

#include "cachelib/interface/components/tests/CacheComponentFactory.h"

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/interface/tests/Utils.h"
#include "cachelib/navy/block_cache/tests/TestHelpers.h"

namespace facebook::cachelib::interface::test {

std::unique_ptr<CacheComponent> RAMCacheFactory::create() {
  auto config = createConfig();
  auto poolConfig = createPoolConfig();
  auto ramCache = ASSERT_OK(
      RAMCacheComponent::create(std::move(config), std::move(poolConfig)));
  return std::make_unique<RAMCacheComponent>(std::move(ramCache));
}

/* static */ LruAllocatorConfig RAMCacheFactory::createConfig() {
  LruAllocatorConfig config;
  config.setCacheName("CacheComponentTest");
  config.setCacheSize(6 * Slab::kSize);
  config.defaultPoolRebalanceStrategy = nullptr;
  return config;
}

/* static */ RAMCacheComponent::PoolConfig RAMCacheFactory::createPoolConfig() {
  static std::set<uint32_t> allocSizes = {64, 128, 256, 512, 1024};
  static MMLru::Config mmConfig{};
  return RAMCacheComponent::PoolConfig{
      .name_ = "test_pool",
      .size_ = allocSizes.size() * Slab::kSize,
      .allocSizes_ = allocSizes,
      .mmConfig_ = mmConfig,
      .ensureProvisionable_ = true,
  };
}

FlashCacheFactory::FlashCacheFactory()
    : hits_(/* count */ kDeviceSize / kRegionSize, /* value */ 0) {}

std::unique_ptr<CacheComponent> FlashCacheFactory::create() {
  auto flashCache = ASSERT_OK(FlashCacheComponent::create(
      "CacheComponentTest", makeConfig(), makeDevice()));
  return std::make_unique<FlashCacheComponent>(std::move(flashCache));
}

/* static */ std::unique_ptr<navy::Device> FlashCacheFactory::makeDevice() {
  return navy::createMemoryDevice(kDeviceSize, /* encryptor */ nullptr);
}

navy::BlockCache::Config FlashCacheFactory::makeConfig() {
  navy::BlockCache::Config config;
  config.regionSize = kRegionSize;
  config.cacheSize = kDeviceSize;
  config.evictionPolicy =
      std::make_unique<::testing::NiceMock<navy::MockPolicy>>(&hits_);
  config.reinsertionConfig.enablePctBased(100);
  return config;
}

std::unique_ptr<CacheComponent> ConsistentFlashCacheFactory::create() {
  auto consistentFlashCache = ASSERT_OK(ConsistentFlashCacheComponent::create(
      "CacheComponentTest", makeConfig(), makeDevice(),
      std::make_unique<MurmurHash2>(), kShardsPower));
  return std::make_unique<ConsistentFlashCacheComponent>(
      std::move(consistentFlashCache));
}

} // namespace facebook::cachelib::interface::test

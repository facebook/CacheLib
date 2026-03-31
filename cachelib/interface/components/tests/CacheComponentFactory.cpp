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
  auto ramCache = ASSERT_OK(createWithPersistence(
      RAMCacheComponent::PersistenceConfig::noPersistenceOrRecovery()));
  return std::make_unique<RAMCacheComponent>(std::move(ramCache));
}

std::unique_ptr<CacheComponent> RAMCacheFactory::createPersistent() {
  auto ramCache = ASSERT_OK(createWithPersistence(
      RAMCacheComponent::PersistenceConfig::persistenceButNoRecovery(
          tmpDir_.path().string())));
  return std::make_unique<RAMCacheComponent>(std::move(ramCache));
}

Result<std::unique_ptr<CacheComponent>> RAMCacheFactory::recover() {
  auto result = createWithPersistence(
      RAMCacheComponent::PersistenceConfig::persistenceAndRecovery(
          tmpDir_.path().string()));
  if (result.hasError()) {
    return folly::makeUnexpected(std::move(result).error());
  }
  return std::make_unique<RAMCacheComponent>(std::move(result).value());
}

Result<RAMCacheComponent> RAMCacheFactory::createWithPersistence(
    RAMCacheComponent::PersistenceConfig pc) {
  return RAMCacheComponent::create(createConfig(), createPoolConfig(),
                                   std::move(pc));
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
  return ASSERT_OK(createWithPersistence(
      makeDevice(kCacheSize), // don't need space for metadata
      FlashCacheComponent::PersistenceConfig::noPersistenceOrRecovery()));
}

std::unique_ptr<CacheComponent> FlashCacheFactory::createPersistent() {
  return ASSERT_OK(createWithPersistence(
      makeDevice(kDeviceSize),
      FlashCacheComponent::PersistenceConfig::persistenceButNoRecovery(
          kMetadataSize)));
}

void FlashCacheFactory::onShutdown(CacheComponent& component) {
  savedDevice_ =
      std::move(static_cast<FlashCacheComponent&>(component).device_);
}

Result<std::unique_ptr<CacheComponent>> FlashCacheFactory::recover() {
  auto device =
      savedDevice_ ? std::move(savedDevice_) : makeDevice(kDeviceSize);
  return createWithPersistence(
      std::move(device),
      FlashCacheComponent::PersistenceConfig::persistenceAndRecovery(
          kMetadataSize));
}

Result<std::unique_ptr<CacheComponent>>
FlashCacheFactory::createWithPersistence(
    std::unique_ptr<navy::Device> device,
    FlashCacheComponent::PersistenceConfig pc) {
  auto result = FlashCacheComponent::create("CacheComponentTest", makeConfig(),
                                            std::move(device), std::move(pc));
  if (result.hasError()) {
    return folly::makeUnexpected(std::move(result).error());
  }
  return std::make_unique<FlashCacheComponent>(std::move(result).value());
}

/* static */ std::unique_ptr<navy::Device> FlashCacheFactory::makeDevice(
    size_t size) {
  return navy::createMemoryDevice(size, /* encryptor */ nullptr, kIOAlignSize);
}

navy::BlockCache::Config FlashCacheFactory::makeConfig() {
  navy::BlockCache::Config config;
  config.regionSize = kRegionSize;
  config.cacheSize = kCacheSize;
  config.indexConfig.setNumSparseMapBuckets(64);
  config.evictionPolicy =
      std::make_unique<::testing::NiceMock<navy::MockPolicy>>(&hits_);
  config.reinsertionConfig.enablePctBased(100);
  return config;
}

Result<std::unique_ptr<CacheComponent>>
ConsistentFlashCacheFactory::createWithPersistence(
    std::unique_ptr<navy::Device> device,
    FlashCacheComponent::PersistenceConfig pc) {
  auto result = ConsistentFlashCacheComponent::create(
      "CacheComponentTest", makeConfig(), std::move(device),
      std::make_unique<MurmurHash2>(), kShardsPower, std::move(pc));
  if (result.hasError()) {
    return folly::makeUnexpected(std::move(result).error());
  }
  return std::make_unique<ConsistentFlashCacheComponent>(
      std::move(result).value());
}

} // namespace facebook::cachelib::interface::test

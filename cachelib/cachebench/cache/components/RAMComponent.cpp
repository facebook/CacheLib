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

#include "cachelib/cachebench/cache/Cache.h"
#include "cachelib/cachebench/cache/components/Components.h"
#include "cachelib/interface/components/RAMCacheComponent.h"

using namespace facebook::cachelib::interface;

namespace facebook::cachelib::cachebench {
namespace {
void validate(const CacheConfig& config) {
  if (config.numPools > 1) {
    throw std::invalid_argument(
        "RAMCacheComponent does not support multiple pools");
  }
}

void setupConfig(LruAllocatorConfig& allocatorConfig,
                 RAMCacheComponent::PoolConfig& poolConfig,
                 const CacheConfig& config) {
  allocatorConfig.enablePoolRebalancing(
      config.getRebalanceStrategy(),
      std::chrono::seconds(config.poolRebalanceIntervalSec));

  if (config.allocSizes.empty()) {
    XDCHECK(config.minAllocSize >= sizeof(CacheValue));
    allocatorConfig.setDefaultAllocSizes(util::generateAllocSizes(
        config.allocFactor, config.maxAllocSize, config.minAllocSize, true));
  } else {
    std::set<uint32_t> allocSizes;
    for (uint64_t s : config.allocSizes) {
      XDCHECK(s >= sizeof(CacheValue));
      allocSizes.insert(s);
    }
    allocatorConfig.setDefaultAllocSizes(std::move(allocSizes));
  }

  // Set hash table config
  allocatorConfig.setAccessConfig(typename LruAllocator::AccessConfig{
      static_cast<uint32_t>(config.htBucketPower),
      static_cast<uint32_t>(config.htLockPower)});

  allocatorConfig.configureChainedItems(typename LruAllocator::AccessConfig{
      static_cast<uint32_t>(config.chainedItemHtBucketPower),
      static_cast<uint32_t>(config.chainedItemHtLockPower)});

  allocatorConfig.setCacheSize(config.cacheSizeMB * (MB));

  if (!config.cacheDir.empty()) {
    allocatorConfig.cacheDir = config.cacheDir;
  }

  if (config.usePosixShm) {
    allocatorConfig.usePosixForShm();
  }

  allocatorConfig.setMemoryLocking(config.lockMemory);

  if (!config.memoryTierConfigs.empty()) {
    allocatorConfig.configureMemoryTiers(config.memoryTierConfigs);
  }

  if (config.enableItemDestructor) {
    auto removeCB = [](const typename LruAllocator::DestructorData&) {};
    allocatorConfig.setItemDestructor(removeCB);
  }

  allocatorConfig.cacheName = kCachebenchCacheName;

  // Set up pool config
  poolConfig.name_ = "pool_1";
  poolConfig.size_ = (config.cacheSizeMB * MB) - (4 * MB);
  poolConfig.mmConfig_ = makeMMConfig<typename LruAllocator::MMConfig>(config);
  poolConfig.ensureProvisionable_ = true;
}
} // namespace

std::unique_ptr<CacheComponent> createRAMCacheComponent(
    const CacheConfig& config) {
  validate(config);

  LruAllocatorConfig lruConfig;
  RAMCacheComponent::PoolConfig poolConfig;
  setupConfig(lruConfig, poolConfig, config);

  auto cache =
      RAMCacheComponent::create(std::move(lruConfig), std::move(poolConfig));
  XCHECK(cache.hasValue()) << "Error creating RAMCacheComponent: "
                           << cache.error();
  return std::make_unique<RAMCacheComponent>(std::move(cache).value());
}

} // namespace facebook::cachelib::cachebench

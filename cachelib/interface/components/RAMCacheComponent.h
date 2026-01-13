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

#include <set>

#include "cachelib/allocator/MMLru.h"
#include "cachelib/interface/CacheComponent.h"

namespace facebook::cachelib {
// Forward declare so we don't have to include headers
template <typename CacheTrait>
class CacheAllocator;
template <typename CacheAllocator>
class CacheAllocatorConfig;

struct LruCacheTrait;
using LruAllocator = CacheAllocator<LruCacheTrait>;
using LruAllocatorConfig = CacheAllocatorConfig<LruAllocator>;

namespace interface {

/**
 * A CacheComponent that uses Cachelib's LruAllocator RAM cache without the
 * flash/NVM cache.
 *
 * Although all APIs use coroutines (according to the CacheComponent interface),
 * they are synchronous under the hood.
 */
class RAMCacheComponent : public CacheComponent {
 public:
  /**
   * Pool configuration. RAMCacheComponent includes only 1 pool - to add more
   * pools, create more RAMCacheComponents.
   */
  struct PoolConfig {
    folly::StringPiece name_{"default"};
    size_t size_;
    const std::set<uint32_t> allocSizes_;
    MMLru::Config mmConfig_;
    std::shared_ptr<RebalanceStrategy> rebalanceStrategy_{nullptr};
    std::shared_ptr<RebalanceStrategy> resizeStrategy_{nullptr};
    bool ensureProvisionable_{false};
  };

  /**
   * Factory method to create a new RAMCacheComponent. Validates the config
   * before actually creating the cache. Also creates a default pool used for
   * all caching in this component.
   *
   * Note: NVM caching or pool rebalancing *must not* be enabled in the config.
   * Returns an error if either are enabled in the config.
   *
   * @param allocConfig the LruAllocatorConfig to use for the cache
   * @param poolConfig the pool configuration for the cache's only pool
   * @return RAMCacheComponent if the config is valid, an error otherwise
   */
  static Result<RAMCacheComponent> create(LruAllocatorConfig&& allocConfig,
                                          PoolConfig&& poolConfig) noexcept;

  /**
   * Escape hatch to allow users to get the underlying LruAllocator. Should be
   * avoided, use the generic APIs if possible.
   *
   * @return the underlying LruAllocator
   */
  LruAllocator& get() const noexcept;

  // ------------------------------ Interface ------------------------------ //

  const std::string& getName() const noexcept override;
  folly::coro::Task<Result<AllocatedHandle>> allocate(
      Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs) override;
  folly::coro::Task<UnitResult> insert(AllocatedHandle&& handle) override;
  folly::coro::Task<Result<std::optional<AllocatedHandle>>> insertOrReplace(
      AllocatedHandle&& handle) override;
  folly::coro::Task<Result<std::optional<ReadHandle>>> find(Key key) override;
  folly::coro::Task<Result<std::optional<WriteHandle>>> findToWrite(
      Key key) override;
  folly::coro::Task<Result<bool>> remove(Key key) override;
  folly::coro::Task<UnitResult> remove(ReadHandle&& handle) override;

 private:
  std::unique_ptr<LruAllocator> cache_;
  PoolId defaultPool_;

  explicit RAMCacheComponent(LruAllocatorConfig&& config);

  // ------------------------------ Interface ------------------------------ //

  UnitResult writeBack(CacheItem& item) override;
  folly::coro::Task<void> release(CacheItem& item, bool inserted) override;
};

} // namespace interface
} // namespace facebook::cachelib

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

#include "cachelib/interface/CacheComponent.h"
#include "cachelib/navy/block_cache/BlockCache.h"

namespace facebook::cachelib::interface {
class FlashCacheItem;

/**
 * A cache component that uses Cachelib's BlockCache flash cache without RAM
 * cache.
 *
 * Cache items are buffered in RAM according to the region config specified in
 * BlockCache::Config. Allocations are parceled out from clean regions in memory
 * and therefore may fail if there are no clean regions available.  Similarly,
 * reads load the region hosting the cache item into RAM.
 *
 * All APIs are cancellable.
 */
class FlashCacheComponent : public CacheComponent {
 public:
  /**
   * Factory method to create a new FlashCacheComponent, i.e., wrapper around
   * BlockCache.  Validates the config before actually creating the cache.
   *
   * @param name name of the flash cache
   * @param config the BlockCache::Config to use for the cache
   * @return FlashCacheComponent if the flash cache could be initialized, an
   * error otherwise.
   */
  static Result<FlashCacheComponent> create(
      std::string name, navy::BlockCache::Config&& config) noexcept;

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
  std::string name_;
  std::unique_ptr<navy::BlockCache> cache_;

  FlashCacheComponent(std::string&& name, navy::BlockCache::Config&& config);

  // Runs func() on a RegionManager worker fiber. Should not be called from an
  // existing region manager worker fiber!
  template <typename FuncT,
            typename ReturnT = std::invoke_result_t<FuncT>,
            typename CleanupFuncT = std::function<void(ReturnT)>>
  folly::coro::Task<ReturnT> onWorkerThread(FuncT&& func,
                                            CleanupFuncT&& cleanup = {});

  using AllocData =
      std::tuple<navy::RegionDescriptor, uint32_t, navy::RelAddress>;
  folly::coro::Task<Result<AllocData>> allocateImpl(const HashedKey& key,
                                                    uint32_t valueSize);
  bool writeBackImpl(CacheItem& item, bool allowReplace);
  folly::coro::Task<UnitResult> insertImpl(AllocatedHandle&& handle,
                                           bool allowReplace);

  // ------------------------------ Interface ------------------------------ //

  UnitResult writeBack(CacheItem& item) override;
  folly::coro::Task<void> release(CacheItem& item, bool inserted) override;

  friend class FlashCacheItem;
};

} // namespace facebook::cachelib::interface

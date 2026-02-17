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
#include "cachelib/interface/utils/ShardedSerializer.h"
#include "cachelib/navy/block_cache/BlockCache.h"

namespace facebook::cachelib::interface {
class FlashCacheItem;
class ConsistentFlashCacheItem;

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
   * @param device the device to use for the cache
   * @return FlashCacheComponent if the flash cache could be initialized, an
   * error otherwise.
   */
  static Result<FlashCacheComponent> create(
      std::string name,
      navy::BlockCache::Config&& config,
      std::unique_ptr<navy::Device> device) noexcept;

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

 protected:
  FlashCacheComponent(std::string&& name,
                      navy::BlockCache::Config&& config,
                      std::unique_ptr<navy::Device> device);

  // Helpers in which the returned cache item is templatized so we can share the
  // implementation with child classes
  template <typename CacheItemT>
  folly::coro::Task<Result<AllocatedHandle>> allocateGeneric(
      Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs);
  template <typename CacheItemT>
  folly::coro::Task<Result<std::optional<WriteHandle>>> findToWriteGeneric(
      Key key);

 private:
  std::string name_;
  // Note: device_ must be declared before cache_ so that it outlives it
  std::unique_ptr<navy::Device> device_;
  std::unique_ptr<navy::BlockCache> cache_;

  // Runs func() on a RegionManager worker fiber. Should not be called from an
  // existing region manager worker fiber!
  template <typename FuncT,
            typename ReturnT = std::invoke_result_t<FuncT>,
            typename CleanupFuncT = std::function<void(ReturnT)>>
  folly::coro::Task<ReturnT> onWorkerThread(FuncT&& func,
                                            CleanupFuncT&& cleanup = {});

  // Helpers used by multiple other APIs
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
  friend class ConsistentFlashCacheItem;
};

/**
 * Same as FlashCacheComponent but provides strong consistency. Shards
 * operations by key and serializes operations per shard to guarantee
 * linearizability.
 *
 * allocate() and findToWrite() return a cache item that holds on to the lock
 * after returning. The locks will get automatically released when the cache
 * item gets destroyed (which should be when the handles get destroyed). This is
 * because these APIs allocate space and store a region descriptor in order to
 * allow writing back after the user has finished modifying the data
 * (insert()/insertOrReplace() for AllocatedHandles, writeBack() for
 * WriteHandles). Holding on to the locks prevents the following race:
 *
 *  - Coro A calls allocate()/findToWrite() and allocates the last space in a
 *    region. It returns while holding on to the region descriptor.
 *  - Coro B calls allocate()/findToWrite(), which grabs the lock. It launches
 *    a fiber to allocate space and blocks waiting for the allocation.
 *  - The fiber sees that the region is full and tries to flush the region. It
 *    blocks, however, because Coro A still has an outstanding reference.
 *  - Coro A tries to write the modified data back to the region. If we didn't
 *    hold on to the lock, it would try to re-acquire the lock and deadlock
 *    since Coro B already has the lock.
 *
 * Coro A holding on to the lock prevents Coro B from grabbing it and launching
 * the allocation fiber (and creating the deadlock cycle).
 *
 * NOTE: ReadHandles returned from find() *do not* hold on to a region
 * descriptor and therefore do not need to hold on to the lock.
 */
class ConsistentFlashCacheComponent : public FlashCacheComponent {
 public:
  /**
   * Factory method to create a new ConsistentFlashCacheComponent.  Validates
   * the config before actually creating the cache.
   *
   * @param name name of the flash cache
   * @param config the BlockCache::Config to use for the cache
   * @param device the device to use for the cache
   * @param hasher the hasher to use for sharding
   * @param shardsPower the number of shards to use (2^shardsPower) when
   * serializing operations, max is ShardedSerializer::kMaxShardsPower
   * @return ConsistentFlashCacheComponent if the flash cache could be
   * initialized, an error otherwise.
   */
  static Result<ConsistentFlashCacheComponent> create(
      std::string name,
      navy::BlockCache::Config&& config,
      std::unique_ptr<navy::Device> device,
      std::unique_ptr<Hash> hasher,
      uint8_t shardsPower) noexcept;

  // ------------------------------ Interface ------------------------------ //

  // don't need to override getName()

  /**
   * Same as FlashCacheComponent::allocate() but runs while holding an exclusive
   * lock. The returned handle holds the lock until it is either inserted or
   * destroyed.
   */
  folly::coro::Task<Result<AllocatedHandle>> allocate(
      Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs) override;

  // don't need to override insert() or insertOrReplace() since AllocatedHandle
  // already holds the appropriate lock

  /**
   * Same as FlashCacheComponent::find() but runs while holding a shared lock.
   * The returned handle *does not* hold on to the lock after returning.
   */
  folly::coro::Task<Result<std::optional<ReadHandle>>> find(Key key) override;

  /**
   * Same as FlashCacheComponent::findToWrite() but runs while holding an
   * exclusive lock. The returned handle holds the lock until it is destroyed
   * (after write back if the handle is marked dirty).
   */
  folly::coro::Task<Result<std::optional<WriteHandle>>> findToWrite(
      Key key) override;

  /**
   * Same as FlashCacheComponent::remove() but runs while holding an exclusive
   * lock.
   */
  folly::coro::Task<Result<bool>> remove(Key key) override;
  folly::coro::Task<UnitResult> remove(ReadHandle&& handle) override;

 private:
  utils::ShardedSerializer serializer_;

  ConsistentFlashCacheComponent(std::string&& name,
                                navy::BlockCache::Config&& config,
                                std::unique_ptr<navy::Device> device,
                                std::unique_ptr<Hash> hasher,
                                uint8_t shardsPower);

  // ------------------------------ Interface ------------------------------ //

  // don't need to override writeBack() since WriteHandle already holds the
  // appropriate lock; don't need to override release() either

  using Base = FlashCacheComponent;
};

} // namespace facebook::cachelib::interface

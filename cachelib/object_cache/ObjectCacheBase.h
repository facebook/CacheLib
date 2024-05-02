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

#include <stdexcept>

#include "cachelib/allocator/Cache.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename AllocatorT>
class ObjectCacheBase : public CacheBase {
  // Get a string referring to the cache name for this cache
  const std::string getCacheName() const override {
    return l1Cache_->getCacheName();
  }

  // Whether it is object-cache.
  bool isObjectCache() const override { return true; }

  // Get the reference  to a memory pool, for stats purposes
  //
  // @param poolId    The pool id to query
  const MemoryPool& getPool(PoolId poolId) const override {
    return l1Cache_->getPool(poolId);
  }

  // Get Pool specific stats (regular pools). This includes stats from the
  // Memory Pool and also the cache.
  //
  // @param poolId   the pool id
  PoolStats getPoolStats(PoolId poolId) const override {
    return l1Cache_->getPoolStats(poolId);
  }

  // @param poolId   the pool id
  AllSlabReleaseEvents getAllSlabReleaseEvents(PoolId poolId) const override {
    return l1Cache_->getAllSlabReleaseEvents(poolId);
  }

  // This can be expensive so it is not part of PoolStats
  //
  // @param pid                   pool id
  // @param slabProjectionLength  number of slabs worth of items to visit to
  //                              estimate the projected age. If 0, returns
  //                              tail age for projection age.
  //
  // @return PoolEvictionAgeStats   see CacheStats.h
  PoolEvictionAgeStats getPoolEvictionAgeStats(
      PoolId pid, unsigned int slabProjectionLength) const override {
    return l1Cache_->getPoolEvictionAgeStats(pid, slabProjectionLength);
  }

  // @return a map of <stat name -> stat value> representation for all the nvm
  // cache stats. This is useful for our monitoring to directly upload them.
  util::StatsMap getNvmCacheStatsMap() const override {
    return l1Cache_->getNvmCacheStatsMap();
  }

  // @return a map of <stat name -> stat value> representation for all the event
  // tracker stats. If no event tracker exists, this will be empty
  std::unordered_map<std::string, uint64_t> getEventTrackerStatsMap()
      const override {
    return l1Cache_->getEventTrackerStatsMap();
  }

  // @return the Cache metadata
  CacheMetadata getCacheMetadata() const noexcept override {
    return l1Cache_->getCacheMetadata();
  }

  // @return cache's memory usage stats
  CacheMemoryStats getCacheMemoryStats() const override {
    return l1Cache_->getCacheMemoryStats();
  }

  // @return the overall cache stats
  GlobalCacheStats getGlobalCacheStats() const override {
    return l1Cache_->getGlobalCacheStats();
  }

  // @return the slab release stats.
  SlabReleaseStats getSlabReleaseStats() const override {
    return l1Cache_->getSlabReleaseStats();
  }

  // return the list of currently active pools that are oversized
  std::set<PoolId> getRegularPoolIdsForResize() const override {
    return l1Cache_->getRegularPoolIdsForResize();
  }

  // return a list of all valid pool ids.
  std::set<PoolId> getPoolIds() const override {
    return l1Cache_->getPoolIds();
  }

  // returns the pool's name by its poolId.
  std::string getPoolName(PoolId poolId) const override {
    return l1Cache_->getPoolName(poolId);
  }

  // returns a list of pools excluding compact cache pools
  std::set<PoolId> getRegularPoolIds() const override {
    return l1Cache_->getRegularPoolIds();
  }

  // returns a list of pools created for ccache.
  std::set<PoolId> getCCachePoolIds() const override { return {}; }

  // return whether a pool participates in auto-resizing
  //
  // not supported in object cache
  bool autoResizeEnabledForPool(PoolId) const override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
    return false;
  }

  // return the virtual interface of an attached  compact cache for a particular
  // pool id
  //
  // not supported in object cache
  const ICompactCache& getCompactCache(PoolId) const override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
  }

 protected:
  // move bytes from one pool to another. The source pool should be at least
  // _bytes_ in size.
  //
  // not supported in object cache
  bool resizePools(PoolId /* src */,
                   PoolId /* dest */,
                   size_t /* bytes */) override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
    return false;
  }

  // resize all of the attached and disabled compact caches
  //
  // not supported in object cache
  void resizeCompactCaches() override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
  }

  // Releases a slab from a pool into its corresponding memory pool
  // or back to the slab allocator, depending on SlabReleaseMode.
  //  SlabReleaseMode::kRebalance -> back to the pool
  //  SlabReleaseMode::kResize -> back to the slab allocator
  //
  // not supported in object cache
  void releaseSlab(PoolId,
                   ClassId,
                   SlabReleaseMode,
                   const void* = nullptr) override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
  }

  // update the number of slabs to be advised
  //
  // not supported in object cache
  void updateNumSlabsToAdvise(int32_t) override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
  }

  // calculate the number of slabs to be advised/reclaimed in each pool
  //
  // not supported in object cache
  PoolAdviseReclaimData calcNumSlabsToAdviseReclaim() override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
    return {};
  }

  // Releasing a slab from this allocation class id and pool id. The release
  // could be for a pool resizing or allocation class rebalancing.
  //
  // not supported in object cache
  void releaseSlab(PoolId,
                   ClassId,
                   ClassId,
                   SlabReleaseMode,
                   const void* = nullptr) override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
  }

  // Reclaim slabs from the slab allocator that were advised away using
  // releaseSlab in SlabReleaseMode::kAdvise mode.
  //
  // not supported in object cache
  unsigned int reclaimSlabs(PoolId, size_t) override {
    throw std::runtime_error("Unsupported function in ObjectCacheBase!");
    return 0;
  }

  std::unique_ptr<AllocatorT> l1Cache_;
};
} // namespace objcache2
} // namespace cachelib
} // namespace facebook

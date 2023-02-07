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

#include <gtest/gtest_prod.h>

#include <mutex>
#include <unordered_map>

#include "cachelib/allocator/CacheDetails.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/ICompactCache.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

class PoolResizer;
class PoolRebalancer;
class PoolOptimizer;
class MemoryMonitor;

// Forward declaration.
class RebalanceStrategy;
class PoolOptimizeStrategy;
class MarginalHitsOptimizeStrategy;
class AllocatorConfigExporter;

namespace tests {
struct SimplePoolOptimizeStrategy;
}

// The mode in which the cache was accessed. This can be used by containers
// to differentiate between the access modes and do appropriate action.
enum class AccessMode { kRead, kWrite };

// used by RemoveCB, indicating if the removal from the MMContainer was an
// eviction or not.
enum class RemoveContext { kEviction, kNormal };

// used by ItemDestructor, indicating how the item is destructed
enum class DestructorContext {
  // item was in dram and evicted from dram. it could have
  // been present in nvm as well.
  kEvictedFromRAM,

  // item was only in nvm and evicted from nvm
  kEvictedFromNVM,

  // item was present in dram and removed by user calling
  // remove()/insertOrReplace, or removed due to expired.
  // it could have been present in nvm as well.
  kRemovedFromRAM,

  // item was present only in nvm and removed by user calling
  // remove()/insertOrReplace.
  kRemovedFromNVM
};

// A base class of cache exposing members and status agnostic of template type.
class CacheBase {
 public:
  CacheBase() = default;
  virtual ~CacheBase() = default;

  // Movable but not copyable
  CacheBase(const CacheBase&) = delete;
  CacheBase& operator=(const CacheBase&) = delete;
  CacheBase(CacheBase&&) = default;
  CacheBase& operator=(CacheBase&&) = default;

  // Get a string referring to the cache name for this cache
  virtual const std::string getCacheName() const = 0;

  // Returns true for ObjectCacheBase, false for CacheAllocator.
  virtual bool isObjectCache() const = 0;

  // Get the reference  to a memory pool, for stats purposes
  //
  // @param poolId    The pool id to query
  virtual const MemoryPool& getPool(PoolId poolId) const = 0;

  // Get Pool specific stats (regular pools). This includes stats from the
  // Memory Pool and also the cache.
  //
  // @param poolId   the pool id
  virtual PoolStats getPoolStats(PoolId poolId) const = 0;

  // @param poolId   the pool id
  virtual AllSlabReleaseEvents getAllSlabReleaseEvents(PoolId poolId) const = 0;

  // This can be expensive so it is not part of PoolStats
  //
  // @param pid                   pool id
  // @param slabProjectionLength  number of slabs worth of items to visit to
  //                              estimate the projected age. If 0, returns
  //                              tail age for projection age.
  //
  // @return PoolEvictionAgeStats   see CacheStats.h
  virtual PoolEvictionAgeStats getPoolEvictionAgeStats(
      PoolId pid, unsigned int slabProjectionLength) const = 0;

  // @return a map of <stat name -> stat value> representation for all the nvm
  // cache stats. This is useful for our monitoring to directly upload them.
  virtual util::StatsMap getNvmCacheStatsMap() const = 0;

  // @return a map of <stat name -> stat value> representation for all the event
  // tracker stats. If no event tracker exists, this will be empty
  virtual std::unordered_map<std::string, uint64_t> getEventTrackerStatsMap()
      const = 0;

  // @return the Cache metadata
  virtual CacheMetadata getCacheMetadata() const noexcept = 0;

  // @return cache's memory usage stats
  virtual CacheMemoryStats getCacheMemoryStats() const = 0;

  // @return the overall cache stats
  virtual GlobalCacheStats getGlobalCacheStats() const = 0;

  // @return the slab release stats.
  virtual SlabReleaseStats getSlabReleaseStats() const = 0;

  // export stats via callback. This function is not thread safe
  //
  // @param statPrefix prefix to be added for stat names
  // @param aggregationInterval interval for delta stats; stat name will be
  //                            suffixed with ".<aggregationInterval>".
  // @param cb callback used to provide the stat

  // Update stats via callback.
  // Each stat name will be suffixed
  void exportStats(const std::string& statPrefix,
                   std::chrono::seconds aggregationInterval,
                   std::function<void(folly::StringPiece, uint64_t)> cb) const;

  // Set rebalancing strategy
  //
  // @param pid Pool id of the pool to set this strategy on.
  // @param strategy The strategy
  void setRebalanceStrategy(PoolId pid,
                            std::shared_ptr<RebalanceStrategy> strategy);

  // @param pid The pool id.
  //
  // @return The rebalancing strategy of the specifid pool.
  std::shared_ptr<RebalanceStrategy> getRebalanceStrategy(PoolId pid) const;

  // Set resizing strategy
  //
  // @param pid Pool id of the pool to set this strategy on.
  // @param strategy The strategy
  void setResizeStrategy(PoolId pid,
                         std::shared_ptr<RebalanceStrategy> strategy);

  // @param pid The pool id.
  //
  // @return The resizing strategy of the specifid pool.
  std::shared_ptr<RebalanceStrategy> getResizeStrategy(PoolId pid) const;

  // Set optimizing strategy
  //
  // @param pid Pool id of the pool to set this strategy on.
  // @param strategy The strategy
  void setPoolOptimizeStrategy(std::shared_ptr<PoolOptimizeStrategy> strategy);

  // @param pid The pool id.
  //
  // @return The optimizing strategy of the specifid pool.
  std::shared_ptr<PoolOptimizeStrategy> getPoolOptimizeStrategy() const;

  // return the list of currently active pools that are oversized
  virtual std::set<PoolId> getRegularPoolIdsForResize() const = 0;

  // return a list of all valid pool ids.
  virtual std::set<PoolId> getPoolIds() const = 0;

  // returns the pool's name by its poolId.
  virtual std::string getPoolName(PoolId poolId) const = 0;

  // returns a list of pools excluding compact cache pools
  virtual std::set<PoolId> getRegularPoolIds() const = 0;

  // returns a list of pools created for ccache.
  virtual std::set<PoolId> getCCachePoolIds() const = 0;

  // return whether a pool participates in auto-resizing
  virtual bool autoResizeEnabledForPool(PoolId) const = 0;

  // return the virtual interface of an attached  compact cache for a particular
  // pool id
  virtual const ICompactCache& getCompactCache(PoolId pid) const = 0;

  // return object cache stats
  virtual void getObjectCacheCounters(const util::CounterVisitor&) const {}

  // <Stat -> Count/Delta> maps
  mutable RateMap counters_;

 protected:
  // move bytes from one pool to another. The source pool should be at least
  // _bytes_ in size.
  //
  //
  // @param src     the pool to be sized down and giving the memory.
  // @param dest    the pool receiving the memory.
  // @param bytes   the number of bytes to move from src to dest.
  // @param   true if the resize succeeded. false if src does does not have
  //          correct size to do the transfer.
  // @throw   std::invalid_argument if src or dest is invalid pool
  virtual bool resizePools(PoolId /* src */,
                           PoolId /* dest */,
                           size_t /* bytes */) = 0;

  // resize all of the attached and disabled compact caches
  virtual void resizeCompactCaches() = 0;

  // serialize cache allocator config for exporting to Scuba
  virtual std::map<std::string, std::string> serializeConfigParams() const = 0;

  // Releases a slab from a pool into its corresponding memory pool
  // or back to the slab allocator, depending on SlabReleaseMode.
  //  SlabReleaseMode::kRebalance -> back to the pool
  //  SlabReleaseMode::kResize -> back to the slab allocator
  //
  // @param pid        Pool that will make make one slab available
  // @param cid        Class that will release a slab back to its pool
  //                   or slab allocator
  // @param mode       the mode for slab release (rebalance/resize)
  // @param hint       hint referring to the slab. this can be an allocation
  //                   that the user knows to exist in the slab. If this is
  //                   nullptr, a random slab is selected from the pool and
  //                   allocation class.
  //
  // @throw std::invalid_argument if the hint is invalid or if the pid or cid
  //        is invalid.
  virtual void releaseSlab(PoolId pid,
                           ClassId cid,
                           SlabReleaseMode mode,
                           const void* hint = nullptr) = 0;

  // update the number of slabs to be advised
  virtual void updateNumSlabsToAdvise(int32_t numSlabsToAdvise) = 0;

  // calculate the number of slabs to be advised/reclaimed in each pool
  virtual PoolAdviseReclaimData calcNumSlabsToAdviseReclaim() = 0;

  // Releasing a slab from this allocation class id and pool id. The release
  // could be for a pool resizing or allocation class rebalancing.
  //
  // All active allocations will be evicted in the process.
  //
  // This function will be blocked until a slab is freed
  //
  // @param pid        Pool that will make one slab available
  // @param victim     Class that will release a slab back to its pool
  //                   or slab allocator or a receiver if defined
  // @param receiver   Class that will receive
  // @param mode  the mode for slab release (rebalance/resize)
  //              the user knows to exist in the slab. If this is nullptr,
  //              a random slab is selected from the pool and allocation
  //              class.
  // @param hint  hint referring to the slab. this can be an allocation
  //              that the user knows to exist in the slab. If this is
  //              nullptr, a random slab is selected from the pool and
  //              allocation class.
  //
  // @throw std::invalid_argument if the hint is invalid or if the pid or cid
  //        is invalid.
  virtual void releaseSlab(PoolId pid,
                           ClassId victim,
                           ClassId receiver,
                           SlabReleaseMode mode,
                           const void* hint = nullptr) = 0;

  // Reclaim slabs from the slab allocator that were advised away using
  // releaseSlab in SlabReleaseMode::kAdvise mode.
  //
  // @param pid        The pool for which to recliam slabs
  // @param numSlabs   The number of slabs to reclaim for the pool
  // @return The number of slabs that were actually reclaimed (<= numSlabs)
  virtual unsigned int reclaimSlabs(PoolId id, size_t numSlabs) = 0;

  // Update pool stats
  //   @param pid    the poolId that needs updating
  void updatePoolStats(const std::string& statPrefix, PoolId pid) const;

  // Update stats specific to compact caches
  void updateCompactCacheStats(const std::string& statPrefix,
                               const ICompactCache& c) const;

  // Update stats specific to the event tracker
  void updateEventTrackerStats(const std::string& statPrefix) const;

  // Update stats specific to NvmCache
  void updateNvmCacheStats(const std::string& statPrefix) const;

  // Update global stats
  void updateGlobalCacheStats(const std::string& statPrefix) const;

  // Update object cache stats
  void updateObjectCacheStats(const std::string& statPrefix) const;

  // Util method to visit estimates
  static void visitEstimates(const util::CounterVisitor& v,
                             const util::PercentileStats::Estimates& est,
                             folly::StringPiece name);

  // return hit rate based on diff between current cache and snapshot values
  struct CacheHitRate {
    double overall;
    double ram;
    double nvm;
  };
  CacheHitRate calculateCacheHitRate(const std::string& statPrefix) const;

  // Protect 'poolRebalanceStragtegies_' and `poolResizeStrategies_`
  // and `poolOptimizeStrategy_`
  mutable std::mutex lock_;
  std::unordered_map<PoolId, std::shared_ptr<RebalanceStrategy>>
      poolRebalanceStrategies_;
  std::unordered_map<PoolId, std::shared_ptr<RebalanceStrategy>>
      poolResizeStrategies_;
  std::shared_ptr<PoolOptimizeStrategy> poolOptimizeStrategy_;

  friend PoolResizer;
  friend PoolRebalancer;
  friend PoolOptimizer;
  friend MemoryMonitor;
  friend AllocatorConfigExporter;
};
} // namespace cachelib
} // namespace facebook

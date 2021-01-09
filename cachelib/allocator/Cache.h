#pragma once

#include <gtest/gtest_prod.h>

#include <mutex>
#include <unordered_map>

#include "cachelib/allocator/CacheDetails.h"
#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/ICompactCache.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/common/Hash.h"

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

// enum value to indicate if the removal from the MMContainer was an eviction
// or not.
enum class RemoveContext { kEviction, kNormal };

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

  // Return a map of <stat name -> stat value> representation for all the nvm
  // cache stats. This is useful for our monitoring to directly upload them.
  virtual std::unordered_map<std::string, double> getNvmCacheStatsMap()
      const = 0;

  // Return a map of <stat name -> stat value> representation for all the event
  // tracker stats. If no event tracker exists, this will be empty
  virtual std::unordered_map<std::string, uint64_t> getEventTrackerStatsMap()
      const = 0;

  // returns the Cache metadata
  virtual CacheMetadata getCacheMetadata() const noexcept = 0;

  // return cache's memory usage stats
  virtual CacheMemoryStats getCacheMemoryStats() const = 0;

  // return the overall cache stats
  virtual GlobalCacheStats getGlobalCacheStats() const = 0;

  virtual SlabReleaseStats getSlabReleaseStats() const = 0;

  void setRebalanceStrategy(PoolId pid,
                            std::shared_ptr<RebalanceStrategy> strategy);
  std::shared_ptr<RebalanceStrategy> getRebalanceStrategy(PoolId pid) const;

  void setResizeStrategy(PoolId pid,
                         std::shared_ptr<RebalanceStrategy> strategy);
  std::shared_ptr<RebalanceStrategy> getResizeStrategy(PoolId pid) const;

  void setPoolOptimizeStrategy(std::shared_ptr<PoolOptimizeStrategy> strategy);
  std::shared_ptr<PoolOptimizeStrategy> getPoolOptimizeStrategy() const;

  // return the list of currently active pools that are oversized
  virtual std::set<PoolId> getRegularPoolIdsForResize() const = 0;

  // return a list of all valid pool ids.
  virtual std::set<PoolId> getPoolIds() const = 0;

  // returns a list of pools excluding compact cache pools
  virtual std::set<PoolId> getRegularPoolIds() const = 0;

  // returns a list of pools created for ccache.
  virtual std::set<PoolId> getCCachePoolIds() const = 0;

  // return whether a pool participates in auto-resizing
  virtual bool autoResizeEnabledForPool(PoolId) const = 0;

  // return the virtual interface of an attached  compact cache for a particular
  // pool id
  virtual const ICompactCache& getCompactCache(PoolId pid) const = 0;

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

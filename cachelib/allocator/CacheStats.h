#pragma once

#include <algorithm>
#include <numeric>

#include "cachelib/allocator/Util.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/allocator/memory/MemoryAllocatorStats.h"
#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/FastStats.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {

// stats class for a single eviction queue
struct EvictionStatPerType {
  // the age of the oldest element in seconds
  uint64_t oldestElementAge = 0ULL;
  // number of elements in the eviction queue
  uint64_t size = 0ULL;
};

// stats class for one MM container (a.k.a one allocation class) related to
// evictions
struct EvictionAgeStat {
  EvictionStatPerType warmQueueStat;

  EvictionStatPerType hotQueueStat;

  EvictionStatPerType coldQueueStat;

  // this is the estimated age after removing a slab worth of elements
  uint64_t projectedAge;
};

// stats related to evictions for a pool
struct PoolEvictionAgeStats {
  // Map from allocation class id to the eviction age stats
  std::unordered_map<ClassId, EvictionAgeStat> classEvictionAgeStats;

  uint64_t getOldestElementAge(ClassId cid) const {
    return classEvictionAgeStats.at(cid).warmQueueStat.oldestElementAge;
  }

  const EvictionStatPerType& getWarmEvictionStat(ClassId cid) const {
    return classEvictionAgeStats.at(cid).warmQueueStat;
  }

  const EvictionStatPerType& getHotEvictionStat(ClassId cid) const {
    return classEvictionAgeStats.at(cid).hotQueueStat;
  }

  const EvictionStatPerType& getColdEvictionStat(ClassId cid) const {
    return classEvictionAgeStats.at(cid).coldQueueStat;
  }

  uint64_t getProjectedAge(ClassId cid) const {
    return classEvictionAgeStats.at(cid).projectedAge;
  }
};

// Stats for MM container
struct MMContainerStat {
  // number of elements in the container.
  size_t size;

  // what is the unix timestamp in seconds of the oldest element existing in
  // the container.
  uint64_t oldestTimeSec;

  // number of lock hits by inserts into the LRU
  uint64_t numLockByInserts;

  // number of lock hits by recordAccess
  uint64_t numLockByRecordAccesses;

  // number of lock hits by removes
  uint64_t numLockByRemoves;

  // refresh time for LRU
  uint64_t lruRefreshTime;

  // TODO: Make the MMContainerStat generic by moving the Lru/2Q specific
  // stats inside MMType and exporting them through a generic stats interface.
  // number of hits in each lru.
  uint64_t numHotAccesses;
  uint64_t numColdAccesses;
  uint64_t numWarmAccesses;
  uint64_t numTailAccesses;
};

// cache related stats for a given allocation class.
struct CacheStat {
  // allocation size for this container.
  uint32_t allocSize;

  // number of attempts to allocate memory
  uint64_t allocAttempts{0};

  // number of failed attempts
  uint64_t allocFailures{0};

  // total fragmented memory size in bytes
  uint64_t fragmentationSize{0};

  // number of hits for this container.
  uint64_t numHits;

  // number of evictions from this class id that was of a chained item
  uint64_t chainedItemEvictions;

  // number of regular items that were evicted from this classId
  uint64_t regularItemEvictions;

  // the stats from the mm container for evictable and unevictable items
  MMContainerStat containerStat;
  MMContainerStat unevictableContainerStat;

  uint64_t numItems() const noexcept {
    return numEvictableItems() + numUnevictableItems();
  }

  // number of elements in this MMContainer
  size_t numEvictableItems() const noexcept { return containerStat.size; }

  size_t numUnevictableItems() const noexcept {
    return unevictableContainerStat.size;
  }

  // total number of evictions.
  uint64_t numEvictions() const noexcept {
    return chainedItemEvictions + regularItemEvictions;
  }

  // the current oldest item in the container in seconds.
  uint64_t getEvictionAge() const noexcept {
    return containerStat.oldestTimeSec != 0
               ? util::getCurrentTimeSec() - containerStat.oldestTimeSec
               : 0;
  }
};

// Stats for a pool
struct PoolStats {
  // pool name given by users of this pool.
  std::string poolName;

  // true if the pool is a compact cache pool.
  bool isCompactCache;

  // total pool size assigned by users when adding pool.
  uint64_t poolSize;

  // total size of the pool that is actively usable, taking advising into
  // account
  uint64_t poolUsableSize;

  // total size of the pool that is set to be advised away.
  uint64_t poolAdvisedSize;

  // container stats that provide evictions etc.
  std::unordered_map<ClassId, CacheStat> cacheStats;

  // stats from the memory allocator perspective. this is a map of MPStat
  // for each allocation class that this pool has.
  MPStats mpStats;

  // number of get hits for this pool.
  uint64_t numPoolGetHits;

  // estimates for eviction age for items in this pool
  util::PercentileStats::Estimates evictionAgeSecs{};

  const std::set<ClassId>& getClassIds() const noexcept {
    return mpStats.classIds;
  }

  // number of attempts to allocate
  uint64_t numAllocAttempts() const;

  // number of attempts that failed
  uint64_t numAllocFailures() const;

  // toal memory fragmentation size of this pool.
  uint64_t totalFragmentation() const;

  // total number of free allocs for this pool
  uint64_t numFreeAllocs() const noexcept;

  // amount of cache memory that is not allocated.
  size_t freeMemoryBytes() const noexcept;

  // number of evictions for this pool
  uint64_t numEvictions() const noexcept;

  // number of all items in this pool
  uint64_t numItems() const noexcept;

  // number of evictable items
  uint64_t numEvictableItems() const noexcept;

  // number of unevictable items
  uint64_t numUnevictableItems() const noexcept;

  // total number of allocations currently in this pool
  uint64_t numActiveAllocs() const noexcept;

  // number of hits for an alloc class in this pool
  uint64_t numHitsForClass(ClassId cid) const {
    return cacheStats.at(cid).numHits;
  }

  // number of slabs in this class id
  uint64_t numSlabsForClass(ClassId cid) const {
    return mpStats.acStats.at(cid).totalSlabs();
  }

  // alloc size corresponding to the class id
  uint32_t allocSizeForClass(ClassId cid) const {
    return cacheStats.at(cid).allocSize;
  }

  // mm container eviction age  for the class
  uint64_t evictionAgeForClass(ClassId cid) const {
    return cacheStats.at(cid).getEvictionAge();
  }

  // total free allocs for the class
  uint64_t numFreeAllocsForClass(ClassId cid) const {
    return mpStats.acStats.at(cid).freeAllocs;
  }

  // This is the real eviction age of this pool as this number
  // guarantees the time any item inserted into this pool will live
  // ignores the classIds that are not used.
  uint64_t minEvictionAge() const;

  // computes the maximum eviction age across all class Ids
  uint64_t maxEvictionAge() const;

  // aggregate this pool stats with another that is compatible. To be
  // compatible, they need to have the same number of classIds
  //
  // throws when the operation is not compatible.
  PoolStats& operator+=(const PoolStats& other);
};

// Stats for slab release events
struct SlabReleaseStats {
  uint64_t numActiveSlabReleases;
  uint64_t numSlabReleaseForRebalance;
  uint64_t numSlabReleaseForResize;
  uint64_t numSlabReleaseForAdvise;
  uint64_t numSlabReleaseForRebalanceAttempts;
  uint64_t numSlabReleaseForResizeAttempts;
  uint64_t numSlabReleaseForAdviseAttempts;
  uint64_t numMoveAttempts;
  uint64_t numMoveSuccesses;
  uint64_t numEvictionAttempts;
  uint64_t numEvictionSuccesses;
};

// Stats for reaper
struct ReaperStats {
  // the total number of items the reaper has visited.
  uint64_t numVisitedItems{0};

  // the number of items reaped.
  uint64_t numReapedItems{0};

  uint64_t numVisitErrs{0};

  // number of times we went through the whole cache
  uint64_t numTraversals{0};

  // indicates the time in ms for the last iteration across the entire cache
  uint64_t lastTraversalTimeMs{0};

  // indicates the maximum of all traversals
  uint64_t minTraversalTimeMs{0};

  // indicates the minimum of all traversals
  uint64_t maxTraversalTimeMs{0};

  // indicates the average of all traversals
  uint64_t avgTraversalTimeMs{0};
};

// CacheMetadata type to export
struct CacheMetadata {
  // allocator_version
  int allocatorVersion;
  // ram_format_version
  int ramFormatVersion;
  // nvm_format_version
  int nvmFormatVersion;
  // cache_total_size
  size_t cacheSize;
};

// forward declaration
namespace detail {
struct Stats;
}

// Stats that apply globally in cache and
// the ones that are aggregated over all pools
struct GlobalCacheStats {
  // number of calls to CacheAllocator::find
  uint64_t numCacheGets{0};

  // number of such calls being a miss in the cache.
  uint64_t numCacheGetMiss{0};

  // number of such calls being an expiry in the cache. This is also included
  // in the numCacheGetMiss stats above.
  uint64_t numCacheGetExpiries{0};

  // number of remove calls to CacheAllocator::remove that requires
  // a lookup first and then remove the item
  uint64_t numCacheRemoves{0};

  // number of remove calls that resulted in a ram hit
  uint64_t numCacheRemoveRamHits{0};

  // number of nvm gets
  uint64_t numNvmGets{0};

  // number of nvm misses
  uint64_t numNvmGetMiss{0};

  // number of nvm misses that happened synchronously
  uint64_t numNvmGetMissFast{0};

  // number of nvm gets that are expired
  uint64_t numNvmGetMissExpired{0};

  // number of gets that joined a concurrent fill for same item
  uint64_t numNvmGetCoalesced{0};

  // number of deletes issues to nvm
  uint64_t numNvmDeletes{0};

  // number of deletes skipped and not issued to nvm
  uint64_t numNvmSkippedDeletes{0};

  // number of writes to nvm
  uint64_t numNvmPuts{0};

  // number of put errors;
  uint64_t numNvmPutErrs{0};

  // number of put failures due to encode call back
  uint64_t numNvmPutEncodeFailure{0};

  // number of puts that observed an inflight delete and aborted
  uint64_t numNvmAbortedPutOnTombstone{0};

  // number of items that are filtered by compaction
  uint64_t numNvmCompactionFiltered{0};

  // number of puts that observed an inflight get and aborted
  uint64_t numNvmAbortedPutOnInflightGet{0};

  // number of evictions from NvmCache
  uint64_t numNvmEvictions{0};

  // number of evictions from nvm that found an inconsistent state in RAM
  uint64_t numNvmUncleanEvict{0};

  // number of evictions that were issued for an item that was in RAM in clean
  // state
  uint64_t numNvmCleanEvict{0};

  // number of evictions that were issued more than once on an unclean item.
  uint64_t numNvmCleanDoubleEvict{0};

  // number of evictions that were already expired
  uint64_t numNvmExpiredEvict{0};

  // number of puts to nvm of a clean item in RAM due to nvm eviction.
  uint64_t numNvmPutFromClean{0};

  // attempts made from nvm cache to allocate an item for promotion
  uint64_t numNvmAllocAttempts{0};

  // number of attempts to allocate an item
  uint64_t allocAttempts{0};

  // number of failures to allocate an item due to internal error
  uint64_t allocFailures{0};

  // number of evictions across all the pools in the cache.
  uint64_t numEvictions{0};

  // number of allocation attempts with invalid input params.
  uint64_t invalidAllocs{0};

  // total number of items
  uint64_t numItems{0};

  // number of refcount overflows
  uint64_t numRefcountOverflow{0};

  // number of allocated items that are permanent
  uint64_t numPermanentItems{0};

  // number of allocated and CHAINED items that are parents (i.e.,
  // consisting of at least one chained child)
  uint64_t numChainedChildItems{0};

  // number of allocated and CHAINED items that are children (i.e.,
  // allocated with a parent handle that it's chained to)
  uint64_t numChainedParentItems{0};

  // number of eviction failures
  uint64_t numEvictionFailureFromAccessContainer{0};
  uint64_t numEvictionFailureFromConcurrentFill{0};
  uint64_t numEvictionFailureFromParentAccessContainer{0};
  uint64_t numEvictionFailureFromMoving{0};
  uint64_t numEvictionFailureFromParentMoving{0};

  // latency and percentile stats of various cachelib operations
  util::PercentileStats::Estimates allocateLatencyNs{};
  util::PercentileStats::Estimates moveChainedLatencyNs{};
  util::PercentileStats::Estimates moveRegularLatencyNs{};
  util::PercentileStats::Estimates nvmLookupLatencyNs{};
  util::PercentileStats::Estimates nvmInsertLatencyNs{};
  util::PercentileStats::Estimates nvmRemoveLatencyNs{};
  util::PercentileStats::Estimates ramEvictionAgeSecs{};
  util::PercentileStats::Estimates ramItemLifeTimeSecs{};
  util::PercentileStats::Estimates nvmSmallLifetimeSecs{};
  util::PercentileStats::Estimates nvmLargeLifetimeSecs{};
  util::PercentileStats::Estimates nvmEvictionSecondsPastExpiry{};
  util::PercentileStats::Estimates nvmEvictionSecondsToExpiry{};
  util::PercentileStats::Estimates nvmPutSize{};

  // time since the ram cache was created in seconds
  uint64_t ramUpTime{0};

  // time since the nvm cache was created in seconds
  uint64_t nvmUpTime{0};

  // if nvmcache is currently active and serving gets
  bool nvmCacheEnabled;

  // stats related to the reaper
  ReaperStats reaperStats;

  uint64_t numNvmRejectsByExpiry{};
  uint64_t numNvmRejectsByClean{};
  uint64_t numNvmRejectsByAP{};

  // Decryption and Encryption errors
  uint64_t numNvmEncryptionErrors{0};
  uint64_t numNvmDecryptionErrors{0};

  // Number of times slab release was aborted due to shutdown
  uint64_t numAbortedSlabReleases{0};

  // current active handles outstanding. This stat should
  // not go to negative. If it's negative, it means we have
  // leaked handles (or some sort of accounting bug internally)
  int64_t numActiveHandles;

  uint64_t numNvmPermItems{0};
};

struct CacheMemoryStats {
  // current memory used for cache in bytes. This excludes the memory used for
  // headers. This can change as memory is advised and reclaimed.
  size_t cacheSize{0};

  // regular pool memory size in bytes
  size_t regularCacheSize{0};

  // compact cache pool memory size in bytes
  size_t compactCacheSize{0};

  // current advised away memory size in bytes.
  size_t advisedSize{0};

  // maximum advised pct of regular cache.
  size_t maxAdvisedPct{0};

  // amount of memory that is not assigned for any pool in bytes
  size_t unReservedSize{0};

  // size of the nvm cache in addition to the ram cache.
  size_t nvmCacheSize{0};

  // returns the advised memory in the unit of slabs.
  size_t numAdvisedSlabs() const { return advisedSize / Slab::kSize; }

  // returne usable portion of the cache size
  size_t usableCacheSize() const { return cacheSize - advisedSize; }

  // amount of memory available on the host
  size_t memAvailableSize{0};

  // rss size of the process
  size_t memRssSize{0};
};

// Stats for compact cache
struct CCacheStats {
  uint64_t get;
  uint64_t getHit;
  uint64_t getMiss;
  uint64_t getErr;
  uint64_t tailHits;

  uint64_t set;
  uint64_t setHit;
  uint64_t setMiss;
  uint64_t setErr;
  uint64_t evictions;

  uint64_t del;
  uint64_t delHit;
  uint64_t delMiss;
  uint64_t delErr;

  uint64_t purgeSuccess;
  uint64_t purgeErr;
  uint64_t lockTimeout;
  uint64_t promoteTimeout;

  double hitRatio() const;

  CCacheStats& operator+=(const CCacheStats& other) {
    get += other.get;
    getHit += other.getHit;
    getMiss += other.getMiss;
    getErr += other.getErr;
    tailHits += other.tailHits;

    set += other.set;
    setHit += other.setHit;
    setMiss += other.setMiss;
    setErr += other.setErr;
    evictions += other.evictions;

    del += other.del;
    delHit += other.delHit;
    delMiss += other.delMiss;
    delErr += other.delErr;

    purgeSuccess += other.purgeSuccess;
    purgeErr += other.purgeErr;
    lockTimeout += other.lockTimeout;
    promoteTimeout += other.promoteTimeout;

    return *this;
  }
};

// Types of background workers
enum PoolWorkerType {
  POOL_REBALANCER = 0,
  POOL_RESIZER,
  MEMORY_MONITOR,
  MAX_POOL_WORKER
};

/* Slab release event data */
struct SlabReleaseData {
  // Time when release occured.
  std::chrono::system_clock::time_point timeOfRelease;
  // The class where the slab was released from.
  ClassId from;
  // The receiver of the released slab.
  ClassId to;
  // The sequence of this event, with respect to other release events logged by
  // this process.
  uint64_t sequenceNum;
  // Time release took.
  uint64_t durationMs;
  // PoolId of the pool where the rebalance occurred.
  PoolId pid;
  // Number of slabs in the victim class after rebalancing.
  unsigned int numSlabsInVictim;
  // Number of slabs in the receiver class after rebalancing.
  unsigned int numSlabsInReceiver;
  // Allocation size of the victim class.
  uint32_t victimAllocSize;
  // Allocation size of the receiver class.
  uint32_t receiverAllocSize;
  // Eviction age of the victim class.
  uint64_t victimEvictionAge;
  // Eviction age of the receiver class.
  uint64_t receiverEvictionAge;
  // Number of free allocs in the victim class
  uint64_t numFreeAllocsInVictim;
};

using SlabReleaseEvents = std::vector<SlabReleaseData>;

//  Slab release events organized by their type
struct AllSlabReleaseEvents {
  SlabReleaseEvents rebalancerEvents;
  SlabReleaseEvents resizerEvents;
  SlabReleaseEvents monitorEvents;
};

} // namespace cachelib
} // namespace facebook

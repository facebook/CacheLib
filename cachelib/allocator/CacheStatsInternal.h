#pragma once
#include <array>
#include <numeric>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {

// forward declaration
struct GlobalCacheStats;

namespace detail {

// collection of stats that are updated at a high frequency, making it
// necessary to track them as thread local counters that are aggregated.
struct Stats {
  // overall hit ratio related stats for the cache.
  // number of calls to CacheAllocator::find
  TLCounter numCacheGets{0};

  // number of such calls being a miss in the cache.
  TLCounter numCacheGetMiss{0};

  // number of such calls being an expiry in the cache. This is also included
  // in the numCacheGetMiss stats above.
  TLCounter numCacheGetExpiries{0};

  // number of remove calls to CacheAllocator::remove that requires
  // a lookup first and then remove the item
  TLCounter numCacheRemoves{0};

  // number of remove calls that resulted in a ram hit
  TLCounter numCacheRemoveRamHits{0};

  // number of nvm gets
  TLCounter numNvmGets{0};

  // number of nvm get miss that happened synchronously
  TLCounter numNvmGetMissFast{0};

  // number of nvm misses
  TLCounter numNvmGetMiss{0};

  // number of nvm gets that are expired
  TLCounter numNvmGetMissExpired{0};

  // number of gets that joined a concurrent fill for same item
  AtomicCounter numNvmGetCoalesced{0};

  // number of deletes issues to nvm
  TLCounter numNvmDeletes{0};

  // number of writes to nvm
  TLCounter numNvmPuts{0};

  // number of put errors;
  TLCounter numNvmPutErrs{0};

  // number of put failures due to encode call back
  AtomicCounter numNvmPutEncodeFailure{0};

  // number of puts that observed an inflight delete and aborted
  AtomicCounter numNvmAbortedPutOnTombstone{0};

  // number of puts that observed an inflight concurrent get and aborted
  AtomicCounter numNvmAbortedPutOnInflightGet{0};

  // number of items that are filtered by compaction
  AtomicCounter numNvmCompactionFiltered{0};

  // number of evictions from NvmCache
  TLCounter numNvmEvictions{0};

  // number of evictions from nvm that found an inconsistent state in RAM
  AtomicCounter numNvmUncleanEvict{0};

  // number of evictions that were issued for an item that was in RAM in clean
  // state
  AtomicCounter numNvmCleanEvict{0};

  // number of evictions that were issued more than once on an unclean item.
  AtomicCounter numNvmCleanDoubleEvict{0};

  // number of evictions that were already expired
  AtomicCounter numNvmExpiredEvict{0};

  // number of entries that were clean in RAM, but evicted and rewritten to
  // nvmcache because the nvmcache version was evicted
  AtomicCounter numNvmPutFromClean{0};

  // Decryption and Encryption errors
  AtomicCounter numNvmEncryptionErrors{0};
  AtomicCounter numNvmDecryptionErrors{0};

  // basic admission policy stats
  TLCounter numNvmRejectsByFilterCb{0};
  TLCounter numNvmRejectsByExpiry{0};
  TLCounter numNvmRejectsByClean{0};
  TLCounter numNvmRejectsByAP{0};

  // attempts made from nvm cache to allocate an item for promotion
  TLCounter numNvmAllocAttempts{0};

  // the number of allocated items that are permanent
  TLCounter numPermanentItems{0};

  // the number of allocated and CHAINED items that are parents (i.e.,
  // consisting of at least one chained child)
  TLCounter numChainedParentItems{0};

  // the number of allocated and CHAINED items that are children (i.e.,
  // allocated with a parent handle that it's chained to)
  TLCounter numChainedChildItems{0};

  AtomicCounter numMoveAttempts{0};
  AtomicCounter numMoveSuccesses{0};
  AtomicCounter numEvictionAttempts{0};
  AtomicCounter numEvictionSuccesses{0};
  AtomicCounter numRefcountOverflow{0};

  // The number of slabs being released right now.
  // This must be zero when `saveState()` is called.
  AtomicCounter numActiveSlabReleases{0};
  AtomicCounter numReleasedForRebalance{0};
  AtomicCounter numReleasedForResize{0};
  AtomicCounter numReleasedForAdvise{0};
  AtomicCounter numAbortedSlabReleases{0};

  // allocations with invalid parameters
  AtomicCounter invalidAllocs{0};

  // latency stats of various cachelib operations
  mutable util::PercentileStats allocateLatency_;
  mutable util::PercentileStats moveChainedLatency_;
  mutable util::PercentileStats moveRegularLatency_;
  mutable util::PercentileStats nvmLookupLatency_;
  mutable util::PercentileStats nvmInsertLatency_;
  mutable util::PercentileStats nvmRemoveLatency_;

  // percentile stats for various cache statistics
  mutable util::PercentileStats ramEvictionAgeSecs_;
  mutable util::PercentileStats ramItemLifeTimeSecs_;
  mutable util::PercentileStats nvmSmallLifetimeSecs_;
  mutable util::PercentileStats nvmLargeLifetimeSecs_;
  mutable util::PercentileStats nvmEvictionSecondsPastExpiry_;
  mutable util::PercentileStats nvmEvictionSecondsToExpiry_;

  // per-pool percentile stats for eviction age
  std::array<util::PercentileStats, MemoryPoolManager::kMaxPools>
      perPoolEvictionAgeSecs_;

  // This tracks in each window what are the percentiles of the sizes of
  // items that we have written to flash. This is at-the-moment view of what
  // we're currently writing into flash.
  mutable util::PercentileStats nvmPutSize_;

  using PerPoolClassAtomicCounters =
      std::array<std::array<AtomicCounter, MemoryAllocator::kMaxClasses>,
                 MemoryPoolManager::kMaxPools>;

  // count of a stat for a specific allocation class
  using PerPoolClassTLCounters =
      std::array<std::array<TLCounter, MemoryAllocator::kMaxClasses>,
                 MemoryPoolManager::kMaxPools>;

  // hit count for every alloc class in every pool
  std::unique_ptr<PerPoolClassTLCounters> cacheHits{};
  std::unique_ptr<PerPoolClassAtomicCounters> allocAttempts{};
  std::unique_ptr<PerPoolClassAtomicCounters> allocFailures{};
  std::unique_ptr<PerPoolClassAtomicCounters> fragmentationSize{};
  std::unique_ptr<PerPoolClassAtomicCounters> chainedItemEvictions{};
  std::unique_ptr<PerPoolClassAtomicCounters> regularItemEvictions{};

  // Eviction failures due to parent cannot be removed from access container
  AtomicCounter evictFailParentAC{0};

  // Eviction failures due to parent cannot be removed because it's being
  // moved
  AtomicCounter evictFailParentMove{0};

  // Eviction failures because this item cannot be removed from access
  // container
  AtomicCounter evictFailAC{0};

  // Eviction failures because this item has a potential concurrent fill
  // from nvm cache. For consistency reason, we cannot evict it. Refer
  // to NvmCache.h for more details.
  AtomicCounter evictFailConcurrentFill{0};

  // Eviction failures because this item is being moved
  AtomicCounter evictFailMove{0};

  // Number of permanent item put in NVM cache.
  AtomicCounter numNvmPermItems{0};

  void init();

  void populateGlobalCacheStats(GlobalCacheStats& ret) const;
};

} // namespace detail
} // namespace cachelib
} // namespace facebook

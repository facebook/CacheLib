#pragma once
#include <array>
#include <numeric>

#include "cachelib/allocator/memory/MemoryAllocator.h"
#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {
namespace detail {

// collection of stats that are updated at a high frequency, making it
// necessary to track them as thread local counters that are aggregated.
struct TLStats {
  // overall hit ratio related stats for the cache.
  // number of calls to CacheAllocator::find
  uint64_t numCacheGets{0};

  // number of such calls being a miss in the cache.
  uint64_t numCacheGetMiss{0};

  // number of nvm gets
  uint64_t numNvmGets{0};

  // number of nvm misses
  uint64_t numNvmGetMiss{0};

  // number of nvm gets that are expired
  uint64_t numNvmGetMissExpired{0};

  // number of gets that joined a concurrent fill for same item
  uint64_t numNvmGetCoalesced{0};

  // number of deletes issues to nvm
  uint64_t numNvmDeletes{0};

  // number of writes to nvm
  uint64_t numNvmPuts{0};

  // number of put errors;
  uint64_t numNvmPutErrs{0};

  // number of put failures due to encode call back
  uint64_t numNvmPutEncodeFailure{0};

  // number of puts that observed an inflight delete and aborted
  uint64_t numNvmAbortedPutOnTombstone{0};

  // number of puts that observed an inflight concurrent get and aborted
  uint64_t numNvmAbortedPutOnInflightGet{0};

  // number of items that are filtered by compaction
  uint64_t numNvmCompactionFiltered{0};

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

  // number of entries that were clean in RAM, but evicted and rewritten to
  // nvmcache because the nvmcache version was evicted
  uint64_t numNvmPutFromClean{0};

  // Decryption and Encryption errors
  uint64_t numNvmEncryptionErrors{0};
  uint64_t numNvmDecryptionErrors{0};

  // the number of allocated items that are permanent
  uint64_t numPermanentItems{0};

  // the number of allocated and CHAINED items that are parents (i.e.,
  // consisting of at least one chained child)
  uint64_t numChainedParentItems{0};

  // the number of allocated and CHAINED items that are children (i.e.,
  // allocated with a parent handle that it's chained to)
  uint64_t numChainedChildItems{0};

  // count of a stat for a specific allocation class
  using ClassCounters = std::array<uint64_t, MemoryAllocator::kMaxClasses>;

  // necessary to ensure that threads can bump these up while we aggregate in
  // another thread.
  static_assert(std::is_same<ClassCounters::value_type, uint64_t>::value,
                "ClassCounters is not compatible");

  static_assert(std::tuple_size<ClassCounters>::value ==
                    MemoryAllocator::kMaxClasses,
                "ClassCounters compile time static size is not sufficient to "
                "hold max alloc classes");

  // hit count for every alloc class in every pool
  std::array<ClassCounters, MemoryPoolManager::kMaxPools> cacheHits{{}};
  TLStats& operator+=(const TLStats& rhs);
};

// collection of internal stats that are updated infrequently that we can
// tolerate atomic overhead or maintaining a thread local copy is memory
// in-efficient due to the number of threads.
struct AtomicStats {
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

  using PerClassAtomicCounters =
      std::array<AtomicCounter, MemoryAllocator::kMaxClasses>;
  using PerPoolClassAtomicCounters =
      std::array<PerClassAtomicCounters, MemoryAllocator::kMaxPools>;

  PerPoolClassAtomicCounters allocAttempts{};
  PerPoolClassAtomicCounters allocFailures{};
  PerPoolClassAtomicCounters fragmentationSize{};
  PerPoolClassAtomicCounters chainedItemEvictions{};
  PerPoolClassAtomicCounters regularItemEvictions{};

  void reset();
  static uint64_t accumulate(const PerClassAtomicCounters& c) {
    return std::accumulate(
        c.begin(),
        c.end(),
        0ULL,
        [](const uint64_t& o, const AtomicCounter& a) { return o + a.get(); });
  }
};

} // namespace detail
} // namespace cachelib
} // namespace facebook

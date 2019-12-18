#include "cachelib/allocator/CacheStats.h"
#include "cachelib/allocator/CacheStatsInternal.h"

namespace facebook {
namespace cachelib {

namespace {
template <int v>
struct SizeVerify {};
} // namespace

namespace detail {

TLStats& TLStats::operator+=(const TLStats& other) {
  numCacheGets += other.numCacheGets;
  numCacheGetMiss += other.numCacheGetMiss;
  numNvmGets += other.numNvmGets;
  numNvmGetMiss += other.numNvmGetMiss;
  numNvmGetMissExpired = other.numNvmGetMissExpired;
  numNvmGetCoalesced += other.numNvmGetCoalesced;
  numNvmDeletes += other.numNvmDeletes;
  numNvmPuts += other.numNvmPuts;
  numNvmPutErrs += other.numNvmPutErrs;
  numNvmPutEncodeFailure += other.numNvmPutEncodeFailure;
  numNvmAbortedPutOnTombstone += other.numNvmAbortedPutOnTombstone;
  numNvmCompactionFiltered += other.numNvmCompactionFiltered;
  numNvmAbortedPutOnInflightGet += other.numNvmAbortedPutOnInflightGet;
  numNvmUncleanEvict += other.numNvmUncleanEvict;
  numNvmCleanEvict += other.numNvmCleanEvict;
  numNvmCleanDoubleEvict += other.numNvmCleanDoubleEvict;
  numNvmExpiredEvict += other.numNvmExpiredEvict;
  numNvmPutFromClean += other.numNvmPutFromClean;
  numNvmEvictions += other.numNvmEvictions;
  numNvmEncryptionErrors += other.numNvmEncryptionErrors;
  numNvmDecryptionErrors += other.numNvmDecryptionErrors;

  for (size_t i = 0; i < cacheHits.size(); i++) {
    for (size_t j = 0; j < cacheHits[i].size(); j++) {
      cacheHits[i][j] += other.cacheHits[i][j];
    }
  }
  return *this;

  // please add any new fields to this operator and increment the size based
  // on the compiler warning.
  SizeVerify<sizeof(TLStats)> a = SizeVerify<65704>{};
  (void)a;
}

void AtomicStats::reset() {
  auto initToZero = [](auto& a) {
    for (auto& s : a) {
      for (auto& c : s) {
        c.set(0);
      }
    }
  };

  initToZero(allocAttempts);
  initToZero(allocFailures);
  initToZero(fragmentationSize);
  initToZero(chainedItemEvictions);
  initToZero(regularItemEvictions);
}

} // namespace detail

PoolStats& PoolStats::operator+=(const PoolStats& other) {
  auto verify = [](bool isCompatible) {
    if (!isCompatible) {
      throw std::invalid_argument(
          "attempting to aggregate incompatible pool stats");
    }
  };

  XDCHECK_EQ(cacheStats.size(), mpStats.acStats.size());

  verify(cacheStats.size() == other.cacheStats.size());
  verify(mpStats.acStats.size() == other.mpStats.acStats.size());
  verify(getClassIds() == other.getClassIds());

  // aggregate mp stats
  {
    auto& d = mpStats;
    const auto& s = other.mpStats;
    d.freeSlabs += s.freeSlabs;
    d.slabsUnAllocated += s.slabsUnAllocated;
    d.numSlabResize += s.numSlabResize;
    d.numSlabRebalance += s.numSlabRebalance;
    d.numSlabAdvise += s.numSlabAdvise;
  }

  for (const ClassId i : other.getClassIds()) {
    verify(cacheStats.at(i).allocSize == other.cacheStats.at(i).allocSize);

    // aggregate CacheStat stats
    {
      auto& d = cacheStats.at(i);
      const auto& s = other.cacheStats.at(i);
      d.allocAttempts += s.allocAttempts;
      d.allocFailures += s.allocFailures;
      d.fragmentationSize += s.fragmentationSize;
      d.numHits += s.numHits;
      d.chainedItemEvictions += s.chainedItemEvictions;
      d.regularItemEvictions += s.regularItemEvictions;
    }

    // aggregate container stats within CacheStat
    {
      auto& d = cacheStats.at(i).containerStat;
      const auto& s = other.cacheStats.at(i).containerStat;
      d.size += s.size;

      if (d.oldestTimeSec < s.oldestTimeSec) {
        d.oldestTimeSec = s.oldestTimeSec;
      }

      d.numLockByInserts += s.numLockByInserts;
      d.numLockByRecordAccesses += s.numLockByRecordAccesses;
      d.numLockByRemoves += s.numLockByRemoves;
      d.numHotAccesses += s.numHotAccesses;
      d.numColdAccesses += s.numColdAccesses;
      d.numWarmAccesses += s.numWarmAccesses;
    }

    // aggregate ac stats
    {
      auto& d = mpStats.acStats.at(i);
      const auto& s = other.mpStats.acStats.at(i);
      // allocsPerSlab is fixed for each allocation class, and thus
      // there is no need to aggregate it
      /* d.allocsPerSlab */
      d.usedSlabs += s.usedSlabs;
      d.freeSlabs += s.freeSlabs;
      d.freeAllocs += s.freeAllocs;
      d.activeAllocs += s.activeAllocs;
      d.full = d.full && s.full ? true : false;
    }
  }

  // aggregate rest of PoolStats
  numPoolGetHits += other.numPoolGetHits;
  return *this;
}

uint64_t PoolStats::numFreeAllocs() const noexcept {
  return mpStats.numFreeAllocs();
}

size_t PoolStats::freeMemoryBytes() const noexcept {
  return mpStats.freeMemory();
}

uint64_t PoolStats::numEvictions() const noexcept {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.numEvictions();
  }
  return n;
}

uint64_t PoolStats::numItems() const noexcept {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.numItems();
  }
  return n;
}

uint64_t PoolStats::numAllocFailures() const {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.allocFailures;
  }
  return n;
}

uint64_t PoolStats::numAllocAttempts() const {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.allocAttempts;
  }
  return n;
}

uint64_t PoolStats::totalFragmentation() const {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.fragmentationSize;
  }
  return n;
}

uint64_t PoolStats::numActiveAllocs() const noexcept {
  uint64_t n = 0;
  for (const auto& ac : mpStats.acStats) {
    n += ac.second.activeAllocs;
  }
  return n;
}

uint64_t PoolStats::minEvictionAge() const {
  if (isCompactCache) {
    return 0;
  }

  XDCHECK_GT(cacheStats.size(), 0u);

  // treat 0 eviction age as higher values so that we filter that out for min.
  // 0 eviction age usually means we dont have anything in that classId. a is
  // min than b only when a is not 0 and is less than b or if b is 0. all
  // other cases, a < b is false.
  return std::min_element(cacheStats.begin(), cacheStats.end(),
                          [](auto& a, auto& b) {
                            uint64_t aVal = a.second.getEvictionAge();
                            uint64_t bVal = b.second.getEvictionAge();
                            return (aVal != 0 && aVal < bVal) || bVal == 0;
                          })
      ->second.getEvictionAge();
}

uint64_t PoolStats::maxEvictionAge() const {
  if (isCompactCache) {
    return 0;
  }

  XDCHECK_GT(cacheStats.size(), 0u);
  return std::max_element(cacheStats.begin(), cacheStats.end(),
                          [](auto& a, auto& b) {
                            return a.second.getEvictionAge() <
                                   b.second.getEvictionAge();
                          })
      ->second.getEvictionAge();
}

namespace {
double hitRatioCalc(uint64_t ops, uint64_t miss) {
  return miss == 0 || ops == 0 ? 100.0
                               : 100.0 - 100.0 * static_cast<double>(miss) /
                                             static_cast<double>(ops);
}
} // namespace

uint64_t PoolStats::numEvictableItems() const noexcept {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.numEvictableItems();
  }
  return n;
}

uint64_t PoolStats::numUnevictableItems() const noexcept {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.numUnevictableItems();
  }
  return n;
}

double CCacheStats::hitRatio() const { return hitRatioCalc(get, getMiss); }

void GlobalCacheStats::initFrom(const detail::TLStats& l) {
  numCacheGets = l.numCacheGets;
  numCacheGetMiss = l.numCacheGetMiss;
  numNvmGets = l.numNvmGets;
  numNvmGetMiss = l.numNvmGetMiss;
  numNvmGetMissExpired = l.numNvmGetMissExpired;
  numNvmGetCoalesced = l.numNvmGetCoalesced;
  numNvmPuts = l.numNvmPuts;
  numNvmDeletes = l.numNvmDeletes;
  numNvmPutErrs = l.numNvmPutErrs;
  numNvmPutEncodeFailure = l.numNvmPutEncodeFailure;
  numNvmAbortedPutOnTombstone += l.numNvmAbortedPutOnTombstone;
  numNvmCompactionFiltered += l.numNvmCompactionFiltered;
  numNvmAbortedPutOnInflightGet = l.numNvmAbortedPutOnInflightGet;
  numNvmUncleanEvict = l.numNvmUncleanEvict;
  numNvmCleanEvict = l.numNvmCleanEvict;
  numNvmCleanDoubleEvict = l.numNvmCleanDoubleEvict;
  numNvmExpiredEvict = l.numNvmExpiredEvict;
  numNvmPutFromClean = l.numNvmPutFromClean;
  numNvmEvictions = l.numNvmEvictions;
  numNvmEncryptionErrors = l.numNvmEncryptionErrors;
  numNvmDecryptionErrors = l.numNvmDecryptionErrors;
}

} // namespace cachelib
} // namespace facebook

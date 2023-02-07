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

#include "cachelib/allocator/CacheStats.h"

#include "cachelib/allocator/CacheStatsInternal.h"

namespace facebook {
namespace cachelib {
namespace detail {

void Stats::init() {
  cacheHits = std::make_unique<PerPoolClassTLCounters>();
  allocAttempts = std::make_unique<PerPoolClassAtomicCounters>();
  evictionAttempts = std::make_unique<PerPoolClassAtomicCounters>();
  fragmentationSize = std::make_unique<PerPoolClassAtomicCounters>();
  allocFailures = std::make_unique<PerPoolClassAtomicCounters>();
  chainedItemEvictions = std::make_unique<PerPoolClassAtomicCounters>();
  regularItemEvictions = std::make_unique<PerPoolClassAtomicCounters>();
  auto initToZero = [](auto& a) {
    for (auto& s : a) {
      for (auto& c : s) {
        c.set(0);
      }
    }
  };

  initToZero(*allocAttempts);
  initToZero(*evictionAttempts);
  initToZero(*allocFailures);
  initToZero(*fragmentationSize);
  initToZero(*chainedItemEvictions);
  initToZero(*regularItemEvictions);
}

template <int>
struct SizeVerify {};

void Stats::populateGlobalCacheStats(GlobalCacheStats& ret) const {
#ifndef SKIP_SIZE_VERIFY
  SizeVerify<sizeof(Stats)> a = SizeVerify<16176>{};
  std::ignore = a;
#endif
  ret.numCacheGets = numCacheGets.get();
  ret.numCacheGetMiss = numCacheGetMiss.get();
  ret.numCacheGetExpiries = numCacheGetExpiries.get();
  ret.numCacheRemoves = numCacheRemoves.get();
  ret.numCacheRemoveRamHits = numCacheRemoveRamHits.get();
  ret.numCacheEvictions = numCacheEvictions.get();
  ret.numRamDestructorCalls = numRamDestructorCalls.get();
  ret.numDestructorExceptions = numDestructorExceptions.get();

  ret.numNvmGets = numNvmGets.get();
  ret.numNvmGetMiss = numNvmGetMiss.get();
  ret.numNvmGetMissFast = numNvmGetMissFast.get();
  ret.numNvmGetMissExpired = numNvmGetMissExpired.get();
  ret.numNvmGetMissDueToInflightRemove = numNvmGetMissDueToInflightRemove.get();
  ret.numNvmGetMissErrs = numNvmGetMissErrs.get();
  ret.numNvmGetCoalesced = numNvmGetCoalesced.get();
  ret.numNvmPuts = numNvmPuts.get();
  ret.numNvmDeletes = numNvmDeletes.get();
  ret.numNvmSkippedDeletes = numNvmSkippedDeletes.get();
  ret.numNvmPutErrs = numNvmPutErrs.get();
  ret.numNvmPutEncodeFailure = numNvmPutEncodeFailure.get();
  ret.numNvmAbortedPutOnTombstone += numNvmAbortedPutOnTombstone.get();
  ret.numNvmCompactionFiltered += numNvmCompactionFiltered.get();
  ret.numNvmAbortedPutOnInflightGet = numNvmAbortedPutOnInflightGet.get();
  ret.numNvmCleanEvict = numNvmCleanEvict.get();
  ret.numNvmCleanDoubleEvict = numNvmCleanDoubleEvict.get();
  ret.numNvmDestructorCalls = numNvmDestructorCalls.get();
  ret.numNvmDestructorRefcountOverflow = numNvmDestructorRefcountOverflow.get();
  ret.numNvmExpiredEvict = numNvmExpiredEvict.get();
  ret.numNvmPutFromClean = numNvmPutFromClean.get();
  ret.numNvmEvictions = numNvmEvictions.get();

  ret.numNvmEncryptionErrors = numNvmEncryptionErrors.get();
  ret.numNvmDecryptionErrors = numNvmDecryptionErrors.get();

  ret.numNvmRejectsByExpiry = numNvmRejectsByExpiry.get();
  ret.numNvmRejectsByClean = numNvmRejectsByClean.get();
  ret.numNvmRejectsByAP = numNvmRejectsByAP.get();

  ret.numChainedParentItems = numChainedParentItems.get();
  ret.numChainedChildItems = numChainedChildItems.get();
  ret.numNvmAllocAttempts = numNvmAllocAttempts.get();
  ret.numNvmAllocForItemDestructor = numNvmAllocForItemDestructor.get();
  ret.numNvmItemDestructorAllocErrors = numNvmItemDestructorAllocErrors.get();

  ret.allocateLatencyNs = this->allocateLatency_.estimate();
  ret.moveChainedLatencyNs = this->moveChainedLatency_.estimate();
  ret.moveRegularLatencyNs = this->moveRegularLatency_.estimate();
  ret.nvmLookupLatencyNs = this->nvmLookupLatency_.estimate();
  ret.nvmInsertLatencyNs = this->nvmInsertLatency_.estimate();
  ret.nvmRemoveLatencyNs = this->nvmRemoveLatency_.estimate();
  ret.ramEvictionAgeSecs = this->ramEvictionAgeSecs_.estimate();
  ret.ramItemLifeTimeSecs = this->ramItemLifeTimeSecs_.estimate();
  ret.nvmSmallLifetimeSecs = this->nvmSmallLifetimeSecs_.estimate();
  ret.nvmLargeLifetimeSecs = this->nvmLargeLifetimeSecs_.estimate();
  ret.nvmEvictionSecondsPastExpiry =
      this->nvmEvictionSecondsPastExpiry_.estimate();
  ret.nvmEvictionSecondsToExpiry = this->nvmEvictionSecondsToExpiry_.estimate();
  ret.nvmPutSize = this->nvmPutSize_.estimate();

  auto accum = [](const PerPoolClassAtomicCounters& c) {
    uint64_t sum = 0;
    for (const auto& x : c) {
      for (const auto& v : x) {
        sum += v.get();
      }
    }
    return sum;
  };
  ret.allocAttempts = accum(*allocAttempts);
  ret.evictionAttempts = accum(*evictionAttempts);
  ret.allocFailures = accum(*allocFailures);
  ret.numEvictions = accum(*chainedItemEvictions);
  ret.numEvictions += accum(*regularItemEvictions);

  ret.invalidAllocs = invalidAllocs.get();
  ret.numRefcountOverflow = numRefcountOverflow.get();

  ret.numEvictionFailureFromAccessContainer = evictFailAC.get();
  ret.numEvictionFailureFromConcurrentFill = evictFailConcurrentFill.get();
  ret.numEvictionFailureFromParentAccessContainer = evictFailParentAC.get();
  ret.numEvictionFailureFromMoving = evictFailMove.get();
  ret.numEvictionFailureFromParentMoving = evictFailParentMove.get();
  ret.numAbortedSlabReleases = numAbortedSlabReleases.get();
  ret.numReaperSkippedSlabs = numReaperSkippedSlabs.get();
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
      d.evictionAttempts += s.evictionAttempts;
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

uint64_t PoolStats::numEvictionAttempts() const {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.evictionAttempts;
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

uint64_t PoolStats::numEvictableItems() const noexcept {
  uint64_t n = 0;
  for (const auto& s : cacheStats) {
    n += s.second.numEvictableItems();
  }
  return n;
}

double CCacheStats::hitRatio() const {
  return util::hitRatioCalc(get, getMiss);
}

void RateMap::updateDelta(const std::string& name, uint64_t value) {
  const uint64_t oldValue = internalCount_[name];
  delta_[name] = util::safeDiff(oldValue, value);
  internalCount_[name] = value;
}

void RateMap::updateCount(const std::string& name, uint64_t value) {
  count_[name] = value;
}

uint64_t RateMap::getDelta(const std::string& name) const {
  auto itr = delta_.find(name);
  if (itr != delta_.end()) {
    return itr->second;
  }
  return 0;
}

void RateMap::exportStats(
    std::chrono::seconds aggregationInterval,
    std::function<void(folly::StringPiece, uint64_t)> cb) {
  for (const auto& pair : count_) {
    cb(pair.first, pair.second);
  }

  const double multiplier = static_cast<double>(kRateInterval.count()) /
                            static_cast<double>(aggregationInterval.count());
  const std::string suffix =
      "." + folly::to<std::string>(kRateInterval.count());
  for (const auto& pair : delta_) {
    cb(pair.first + suffix,
       util::narrow_cast<uint64_t>(pair.second * multiplier));
  }
}

} // namespace cachelib
} // namespace facebook

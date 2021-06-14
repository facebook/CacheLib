#include "cachelib/cachebench/util/CacheConfig.h"

#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/RandomStrategy.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
CacheConfig::CacheConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, allocator);
  JSONSetVal(configJson, cacheSizeMB);
  JSONSetVal(configJson, poolRebalanceIntervalSec);
  JSONSetVal(configJson, moveOnSlabRelease);
  JSONSetVal(configJson, rebalanceStrategy);
  JSONSetVal(configJson, rebalanceMinSlabs);
  JSONSetVal(configJson, rebalanceDiffRatio);

  JSONSetVal(configJson, htBucketPower);
  JSONSetVal(configJson, htLockPower);

  JSONSetVal(configJson, lruRefreshSec);
  JSONSetVal(configJson, lruRefreshRatio);
  JSONSetVal(configJson, mmReconfigureIntervalSecs);
  JSONSetVal(configJson, lruUpdateOnWrite);
  JSONSetVal(configJson, lruUpdateOnRead);
  JSONSetVal(configJson, tryLockUpdate);
  JSONSetVal(configJson, lruIpSpec);

  JSONSetVal(configJson, lru2qHotPct);
  JSONSetVal(configJson, lru2qColdPct);

  JSONSetVal(configJson, allocFactor);
  JSONSetVal(configJson, maxAllocSize);
  JSONSetVal(configJson, minAllocSize);
  JSONSetVal(configJson, allocSizes);

  JSONSetVal(configJson, numPools);
  JSONSetVal(configJson, poolSizes);

  JSONSetVal(configJson, dipperSizeMB);
  JSONSetVal(configJson, devicePaths);
  JSONSetVal(configJson, writeAmpDeviceList);

  JSONSetVal(configJson, dipperNavyBlock);
  JSONSetVal(configJson, dipperNavyUseRegionLru);
  JSONSetVal(configJson, navySegmentedFifoSegmentRatio);
  JSONSetVal(configJson, dipperNavySizeClasses);
  JSONSetVal(configJson, dipperNavyReqOrderShardsPower);
  JSONSetVal(configJson, dipperNavyBigHashSizePct);
  JSONSetVal(configJson, dipperNavyBigHashBucketSize);
  JSONSetVal(configJson, dipperNavyBloomFilterPerBucketSize);
  JSONSetVal(configJson, dipperNavySmallItemMaxSize);
  JSONSetVal(configJson, dipperNavyParcelMemoryMB);
  JSONSetVal(configJson, dipperNavyUseStackAllocation);
  JSONSetVal(configJson, dipperNavyStackAllocReadBufSizeKB);
  JSONSetVal(configJson, navyHitsReinsertionThreshold);
  JSONSetVal(configJson, navyProbabilityReinsertionThreshold);
  JSONSetVal(configJson, navyReaderThreads);
  JSONSetVal(configJson, navyWriterThreads);
  JSONSetVal(configJson, navyCleanRegions);
  JSONSetVal(configJson, navyAdmissionWriteRateMB);
  JSONSetVal(configJson, navyMaxConcurrentInserts);
  JSONSetVal(configJson, navyDataChecksum);
  JSONSetVal(configJson, navyNumInmemBuffers);
  JSONSetVal(configJson, truncateItemToOriginalAllocSizeInNvm);
  JSONSetVal(configJson, navyEncryption);

  JSONSetVal(configJson, memoryOnlyTTL);

  JSONSetVal(configJson, mlNvmAdmissionPolicy);
  JSONSetVal(configJson, mlNvmAdmissionPolicyLocation);

  JSONSetVal(configJson, useTraceTimeStamp);
  JSONSetVal(configJson, printNvmCounters);
  JSONSetVal(configJson, tickerSynchingSeconds);
  JSONSetVal(configJson, enableItemDestructorCheck);
  JSONSetVal(configJson, enableItemDestructor);

  // if you added new fields to the configuration, update the JSONSetVal
  // to make them available for the json configs and increment the size
  // below
  checkCorrectSize<CacheConfig, 688>();

  if (numPools != poolSizes.size()) {
    throw std::invalid_argument(folly::sformat(
        "number of pools must be the same as the pool size distribution. "
        "numPools: {}, poolSizes.size(): {}",
        numPools, poolSizes.size()));
  }
}

std::shared_ptr<RebalanceStrategy> CacheConfig::getRebalanceStrategy() {
  if (poolRebalanceIntervalSec == 0) {
    return nullptr;
  }

  if (rebalanceStrategy == "tail-age") {
    auto config = LruTailAgeStrategy::Config{
        rebalanceDiffRatio, static_cast<unsigned int>(rebalanceMinSlabs)};
    return std::make_shared<LruTailAgeStrategy>(config);
  } else if (rebalanceStrategy == "hits") {
    auto config = HitsPerSlabStrategy::Config{
        rebalanceDiffRatio, static_cast<unsigned int>(rebalanceMinSlabs)};
    return std::make_shared<HitsPerSlabStrategy>(config);
  } else {
    // use random strategy to just trigger some slab release.
    return std::make_shared<RandomStrategy>(
        RandomStrategy::Config{static_cast<unsigned int>(rebalanceMinSlabs)});
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

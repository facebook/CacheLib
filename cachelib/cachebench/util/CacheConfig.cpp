#include "cachelib/cachebench/util/CacheConfig.h"

#include "cachelib/allocator/HitsPerSlabStrategy.h"
#include "cachelib/allocator/LruTailAgeStrategy.h"
#include "cachelib/allocator/RandomStrategy.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
CacheConfig::CacheConfig(const folly::dynamic& configJson) {
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
  JSONSetVal(configJson, allocFactor);
  JSONSetVal(configJson, allocSizes);

  JSONSetVal(configJson, numPools);
  JSONSetVal(configJson, poolSizes);

  JSONSetVal(configJson, provisionPool);

  JSONSetVal(configJson, dipperSizeMB);
  JSONSetVal(configJson, dipperBackend);
  JSONSetVal(configJson, dipperFilePath);
  JSONSetVal(configJson, dipperNavyUseMemoryDevice);
  JSONSetVal(configJson, dipperDevicePath);
  JSONSetVal(configJson, writeAmpDeviceList);
  JSONSetVal(configJson, dipperUseDirectIO);
  JSONSetVal(configJson, dipperBloomSizeMB);
  JSONSetVal(configJson, dipperAsyncThreads);
  JSONSetVal(configJson, dipperBucketSizeKB);

  JSONSetVal(configJson, dipperNavyBlock);
  JSONSetVal(configJson, dipperNavyUseRegionLru);
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

  JSONSetVal(configJson, lru2qHotPct);
  JSONSetVal(configJson, lru2qColdPct);

  JSONSetVal(configJson, truncateItemToOriginalAllocSizeInNvm);

  // if you added new fields to the configuration, update the JSONSetVal
  // to make them available for the json configs and increment the size
  // below
  checkCorrectSize<CacheConfig, 528>();

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

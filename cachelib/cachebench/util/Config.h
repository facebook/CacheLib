#pragma once
#include <unordered_map>

#include "cachelib/cachebench/util/CacheConfig.h"
#include "cachelib/cachebench/util/JSONConfig.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

struct DistributionConfig : public JSONConfig {
  explicit DistributionConfig(const folly::dynamic& configJson,
                              const std::string& configPath);

  // for test purposes
  DistributionConfig() {}

  // Key size distribution
  std::vector<double> keySizeRange{};
  std::vector<double> keySizeRangeProbability{};

  // Value size distribution
  std::vector<double> valSizeRange{};
  std::vector<double> valSizeRangeProbability{};

  // If specified, a file containing a size distribution to be
  // loaded by the distribution
  std::string valSizeDistFile{};

  // Chained item length distribution
  std::vector<double> chainedItemLengthRange{};
  std::vector<double> chainedItemLengthRangeProbability{};

  // Chained item value size distribution
  std::vector<double> chainedItemValSizeRange{};
  std::vector<double> chainedItemValSizeRangeProbability{};

  // Popularity distribution shape.  This is defined by a set of weighted
  // buckets. popularityBuckets defines the number of objects in a bucket, and
  // popularityWeights defines the weight of each buckets.
  std::vector<size_t> popularityBuckets{};
  std::vector<double> popularityWeights{};

  // If specified, a file containing a popularity distribution to be
  // loaded by the distribution
  std::string popDistFile{};

  // Operation distribution
  double getRatio{0.0};
  double setRatio{0.0};
  double delRatio{0.0};
  double addChainedRatio{0.0};
  double loneGetRatio{0.0};
  double loneSetRatio{0.0};

  bool usesChainedItems() const { return addChainedRatio > 0; }
};

struct StressorConfig : public JSONConfig {
  // by default everything is stress test. When @mode == "integration",
  // the test case specified in @name will control how to create a cache and run
  // the test. The other settings, except @numOps, will not take any effect.
  std::string mode{"stress"};
  // by defaullt, lru allocator
  std::string allocator{"LRU"};
  // Which workload generator to use, default is
  // workload generator which samples from some distribution
  // but "replay" allows replaying a production trace, for example.
  std::string generator{};
  // When using sampling based methods, which
  // distributions to use.  Default is RangeDistribution
  // which uses specified piecewise constant distributions
  std::string distribution{};

  // name identifying the type of the stressor.
  std::string name;

  // follow get misses with a set
  bool enableLookaside{false};

  // if enabled, initializes  an item with random bytes. For consistency mode,
  // this option is ignored since the consistency check fills in a sequence
  // number into the item.
  bool populateItem{true};

  // interval in milliseconds between taking a snapshot of the stats
  uint64_t samplingIntervalMs{1000};

  // if enabled, the cache will be filled up before test is run
  // TODO: support prepopulating Chained Items going forward
  bool prepopulateCache{false};

  // number of threads to use for prepopulating the cache. 0 means the same as
  // the numThreads below which is used for benchmarking.
  uint64_t prepopulateThreads{0};

  // time to sleep between operations every 8 inserts in every threads.
  uint64_t prepopulateSleepMs{0};

  // If enabled, stressor will verify operations' results are consistent.
  bool checkConsistency{false};

  uint64_t numOps{0};     // operation per thread
  uint64_t numThreads{0}; // number of threads that will run
  uint64_t numKeys{0};    // number of keys that will be used

  uint64_t opDelayBatch{0}; // how many requests before a delay is added
  uint64_t opDelayNs{0};    // nanoseconds delay between each operation

  // Distribution of operations across the pools in cache
  // This cannot exceed the number of pools in cache
  std::vector<double> opPoolDistribution{1.0};

  // Distribution of keys across pools in cache (1 key only belongs to 1 pool)
  std::vector<double> keyPoolDistribution{1.0};

  // Max number of inconsistency detection (exits after exceeded).
  uint64_t maxInconsistencyCount{50};

  // Trace file containing the operations for more accurate replay
  std::string traceFileName{};

  // location of the path for the files referenced inside the json. If not
  // specified, it defaults to the path of the json file being parsed.
  std::string configPath{};

  // Configs for piecewise caching in which we split a huge content into
  // multiple pieces.
  // cachePieceSize: size of each piece
  // maxCachePieces: maximum number of pieces we cache for a single content
  uint64_t cachePieceSize{65536}; // 64KB
  uint64_t maxCachePieces{32000}; // 32000 * 64KB = 2GB

  // Allows multiple distributions, one corresponding to each pool of workload
  // in the cache.
  std::vector<DistributionConfig> poolDistributions;

  StressorConfig() {}
  explicit StressorConfig(const folly::dynamic& configJson);

  bool usesChainedItems() const;
};

// Configs for setting up the cache allocator and specify load test parameters
class CacheBenchConfig {
 public:
  CacheBenchConfig(CacheConfig cacheConfig, StressorConfig testConfig);
  explicit CacheBenchConfig(const std::string& path);

  const CacheConfig& getCacheConfig() const { return cacheConfig_; }
  const StressorConfig& getTestConfig() const { return testConfig_; }

 private:
  CacheConfig cacheConfig_;
  StressorConfig testConfig_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

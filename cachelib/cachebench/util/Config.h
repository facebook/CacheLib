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

  // for continuous value sizes, the probability is expressed per interval
  // instead of per value size range.
  bool usesDiscreteValueSizes() const {
    return valSizeRange.size() == valSizeRangeProbability.size();
  }

  bool usesDiscretePopularity() const {
    return popularityBuckets.size() && popularityWeights.size();
  }
};

struct ReplayGeneratorConfig : public JSONConfig {
  ReplayGeneratorConfig() {}

  explicit ReplayGeneratorConfig(const folly::dynamic& configJson);

  // serializeMode determines how/whether we serialize requests with same
  // key for replaying. Need to be one of the 3:
  // strict: requests for same key are serialized, so they are processed
  //         sequentially
  // relaxed: requests for same key with certain time interval are serialized
  // none: no guarantee
  enum class SerializeMode {
    strict = 0,
    relaxed,
    none,
  };
  std::string replaySerializationMode{"strict"};

  // The time interval threshold when replaySerializationMode is relaxed.
  uint64_t relaxedSerialIntervalMs{500};

  // # of extra fields in trace sample that we track broken down stats.
  // These fields are placed after existing fields:
  // https://fburl.com/diffusion/hl3qerc9
  uint32_t numAggregationFields{0};

  // # of extra fields after the defined fields and numAggregationFields in
  // trace sample. E.g., additional ML features can be put here
  uint32_t numExtraFields{0};

  // For each aggregation field, we track the statistics broken down by
  // specific aggregation values. this map specifies the values for which
  // stats are aggregated by per field.
  // Mapping: field index in the aggregation fields (starting at 0) -->
  // list of values we track for that field
  std::unordered_map<uint32_t, std::vector<std::string>> statsPerAggField;

  SerializeMode getSerializationMode() const;
};

struct StressorConfig : public JSONConfig {
  // by default everything is stress test. When @mode == "integration",
  // the test case specified in @name will control how to create a cache and run
  // the test. The other settings, except @numOps, will not take any effect.
  std::string mode{"stress"};

  // Which workload generator to use, default is
  // workload generator which samples from some distribution
  // but "replay" allows replaying a production trace, for example.
  std::string generator{};

  // Valid when generator is replay generator
  ReplayGeneratorConfig replayGeneratorConfig;

  // name identifying the type of the stressor.
  std::string name;

  // follow get misses with a set
  bool enableLookaside{false};

  // if enabled, initializes an item with random bytes. For consistency mode,
  // this option is ignored since the consistency check fills in a sequence
  // number into the item.
  bool populateItem{true};

  // interval in milliseconds between taking a snapshot of the stats
  uint64_t samplingIntervalMs{1000};

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
  // Supported formats include specifying an absolute filename and filename
  // relative to the configPath
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

  // If enabled and using trace replay mode. We will repeat the trace file again
  // and again until the number of operations specified in the test config.
  bool repeatTraceReplay{false};

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

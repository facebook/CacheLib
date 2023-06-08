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
  // The value size will be changed to max(valSize, sizeof(CacheValue))
  // when allocate in the cache for cachebench. If the size distribution is
  // important to the test, this may affect the test.
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
  double updateRatio{0.0};
  double couldExistRatio{0.0};

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

struct MLAdmissionConfig : public JSONConfig {
  MLAdmissionConfig() {}

  explicit MLAdmissionConfig(const folly::dynamic& configJson);

  std::string modelPath;

  // Map from feature name to its corresponding index in sample fields.
  // Note these features could be in defined fields, aggregation Fields,
  // or ExtraFields.
  // Support two kinds of features:
  // numeric features, categorical features.
  std::unordered_map<std::string, uint32_t> numericFeatures;
  std::unordered_map<std::string, uint32_t> categoricalFeatures;

  double targetRecall;
  size_t admitCategory;
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

  uint32_t ampFactor{1};

  // The time interval threshold when replaySerializationMode is relaxed.
  uint64_t relaxedSerialIntervalMs{500};

  // # of extra fields in trace sample. These are used to offer break down of
  // stats by some workloadload generators.  These fields are placed after
  // defined fields
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

  std::shared_ptr<MLAdmissionConfig> mlAdmissionConfig;

  SerializeMode getSerializationMode() const;
};

// The class defines the admission policy at stressor level. The stressor
// checks the admission policy first before inserting an item into cache.
//
// This base class always returns true, allowing the insersion.
class StressorAdmPolicy {
 public:
  virtual ~StressorAdmPolicy() = default;

  virtual bool accept(
      const std::unordered_map<std::string, std::string>& /*featureMap*/) {
    return true;
  }
};

struct StressorConfig : public JSONConfig {
  // Which workload generator to use, default is
  // workload generator which samples from some distribution
  // but "replay" allows replaying a production trace, for example.
  std::string generator{};

  // Valid when generator is replay generator
  ReplayGeneratorConfig replayGeneratorConfig;

  // name identifying a custom type of the stress test. When empty, launches a
  // standard stress test using the workload config against an instance of the
  // cache defined by the CacheConfig. Other supported options are
  // "high_refcount", "cachelib_map", cachelib_range_map", "fast_shutdown",
  // "async"
  std::string name;

  // follow get misses with a set
  bool enableLookaside{false};

  // ignore opCount and does not repeat operations
  bool ignoreOpCount{false};

  // only set a key in the cache if the key already doesn't exist
  // this is useful for replaying traces with both get and set, and also
  // for manually configured synthetic workloads.
  bool onlySetIfMiss{false};

  // if enabled, initializes an item with random bytes. For consistency mode,
  // this option is ignored since the consistency check fills in a sequence
  // number into the item.
  bool populateItem{true};

  // interval in milliseconds between taking a snapshot of the stats
  uint64_t samplingIntervalMs{1000};

  // If enabled, stressor will verify operations' results are consistent.
  bool checkConsistency{false};

  // If enabled, stressor will check whether nvm cache has been warmed up and
  // output stats after warmup.
  bool checkNvmCacheWarmUp{false};

  // If enabled, each value will be read on find. This is useful for measuring
  // performance of value access.
  bool touchValue{false};

  uint64_t numOps{0};     // operation per thread
  uint64_t numThreads{0}; // number of threads that will run
  uint64_t numKeys{0};    // number of keys that will be used

  // Req generation throttling delay for each thread; those generated reqs are
  // subject to an additional global rate shaping specified below (opRatePerSec)
  uint64_t opDelayBatch{0}; // how many requests before a delay is added
  uint64_t opDelayNs{0};    // nanoseconds delay between each operation

  // Max overall number of operations per second (global); the token bucket
  // is used for limiting/shaping the global req rate
  uint64_t opRatePerSec{0};
  uint64_t opRateBurstSize{0};

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
  std::vector<std::string> traceFileNames{};

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

  // Max number of invalid destructor detection (destructor call more than once
  // for an item or wrong version).
  uint64_t maxInvalidDestructorCount{50};

  // Allows multiple distributions, one corresponding to each pool of workload
  // in the cache.
  std::vector<DistributionConfig> poolDistributions;

  // Factor to be divided from the timestamp to get to unit "second"
  // By default, timestamps are in milliseconds.
  uint64_t timestampFactor{1000};

  // admission policy for cache.
  std::shared_ptr<StressorAdmPolicy> admPolicy{};

  StressorConfig() {}
  explicit StressorConfig(const folly::dynamic& configJson);

  // return true if the workload configuration uses chained items.
  bool usesChainedItems() const;
};

// user defined function to configure parts of cache config outside of the
// json
using CacheConfigCustomizer = std::function<CacheConfig(CacheConfig)>;

// user defined function to configure parts of stressor config outside of the
// json
using StressorConfigCustomizer = std::function<StressorConfig(StressorConfig)>;

// Configs for setting up the cache allocator and specify load test parameters
class CacheBenchConfig {
 public:
  // read the json file in the path and intialize the cache bench
  // configuration. Some fb specific configuration is abstracted out of the
  // json file and is set through a custom setup to keep the dependencies
  // separate.
  //
  // @param path   path for the json file
  // @param c      the customization function for cache config (optional)
  // @param s      the customization function for stressor config (optional)
  explicit CacheBenchConfig(const std::string& path,
                            CacheConfigCustomizer c = {},
                            StressorConfigCustomizer s = {});

  const CacheConfig& getCacheConfig() const { return cacheConfig_; }
  const StressorConfig& getStressorConfig() const { return stressorConfig_; }

 private:
  CacheConfig cacheConfig_;
  StressorConfig stressorConfig_;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

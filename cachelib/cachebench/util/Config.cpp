#include "cachelib/cachebench/util/Config.h"

#include <folly/FileUtil.h>
#include <folly/json.h>
#include <unordered_map>

namespace facebook {
namespace cachelib {
namespace cachebench {
StressorConfig::StressorConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, mode);
  JSONSetVal(configJson, allocator);
  JSONSetVal(configJson, generator);
  JSONSetVal(configJson, distribution);

  JSONSetVal(configJson, name);

  JSONSetVal(configJson, enableLookaside);
  JSONSetVal(configJson, populateItem);
  JSONSetVal(configJson, samplingIntervalMs);

  JSONSetVal(configJson, checkConsistency);

  JSONSetVal(configJson, numOps);
  JSONSetVal(configJson, numThreads);
  JSONSetVal(configJson, numKeys);

  JSONSetVal(configJson, opDelayBatch);
  JSONSetVal(configJson, opDelayNs);

  JSONSetVal(configJson, opPoolDistribution);
  JSONSetVal(configJson, keyPoolDistribution);

  JSONSetVal(configJson, maxInconsistencyCount);

  JSONSetVal(configJson, traceFileName);
  JSONSetVal(configJson, configPath);

  JSONSetVal(configJson, cachePieceSize);
  JSONSetVal(configJson, maxCachePieces);

  JSONSetVal(configJson, repeatTraceReplay);

  if (configJson.count("poolDistributions")) {
    for (auto& it : configJson["poolDistributions"]) {
      poolDistributions.push_back(DistributionConfig(it, configPath));
    }
  } else {
    poolDistributions.push_back(DistributionConfig(configJson, configPath));
  }

  if (configJson.count("replayGeneratorConfig")) {
    replayGeneratorConfig =
        ReplayGeneratorConfig{configJson["replayGeneratorConfig"]};
  }

  // If you added new fields to the configuration, update the JSONSetVal
  // to make them available for the json configs and increment the size
  // below
  checkCorrectSize<StressorConfig, 496>();
}

bool StressorConfig::usesChainedItems() const {
  for (const auto& c : poolDistributions) {
    if (c.usesChainedItems()) {
      return true;
    }
  }
  return false;
}

CacheBenchConfig::CacheBenchConfig(CacheConfig cacheConfig,
                                   StressorConfig testConfig)
    : cacheConfig_(cacheConfig), testConfig_(testConfig) {}

CacheBenchConfig::CacheBenchConfig(const std::string& path) {
  std::string configString;
  if (!folly::readFile(path.c_str(), configString)) {
    throw std::invalid_argument(
        folly::sformat("could not read file: {}", path));
  }

  std::cout << "===JSON Config===" << std::endl;
  std::cout << configString << std::endl;

  auto configJson = folly::parseJson(configString);
  auto& testConfigJson = configJson["test_config"];
  if (testConfigJson.getDefault("configPath", "").empty()) {
    // strip out the file name at the end to get  the directory.
    const std::string configDir{path.begin(),
                                path.begin() + path.find_last_of('/')};
    testConfigJson["configPath"] = configDir;
  }

  testConfig_ = StressorConfig{testConfigJson};
  cacheConfig_ = CacheConfig{configJson["cache_config"]};
}

DistributionConfig::DistributionConfig(const folly::dynamic& jsonConfig,
                                       const std::string& configPath) {
  JSONSetVal(jsonConfig, keySizeRange);
  JSONSetVal(jsonConfig, keySizeRangeProbability);

  JSONSetVal(jsonConfig, valSizeRange);
  JSONSetVal(jsonConfig, valSizeRangeProbability);

  JSONSetVal(jsonConfig, valSizeDistFile);

  JSONSetVal(jsonConfig, chainedItemLengthRange);
  JSONSetVal(jsonConfig, chainedItemLengthRangeProbability);

  JSONSetVal(jsonConfig, chainedItemValSizeRange);
  JSONSetVal(jsonConfig, chainedItemValSizeRangeProbability);

  JSONSetVal(jsonConfig, popularityBuckets);
  JSONSetVal(jsonConfig, popularityWeights);

  JSONSetVal(jsonConfig, popDistFile);

  JSONSetVal(jsonConfig, getRatio);
  JSONSetVal(jsonConfig, setRatio);
  JSONSetVal(jsonConfig, delRatio);
  JSONSetVal(jsonConfig, addChainedRatio);
  JSONSetVal(jsonConfig, loneGetRatio);
  JSONSetVal(jsonConfig, loneSetRatio);

  auto readFile = [&](const std::string& f) {
    std::string str;
    const std::string path = folly::sformat("{}/{}", configPath, f);
    std::cout << "reading distribution params from " << path << std::endl;
    if (!folly::readFile(path.c_str(), str)) {
      throw std::invalid_argument(
          folly::sformat("could not read file: {}", path));
    }
    return str;
  };

  if (!valSizeDistFile.empty()) {
    const auto configJsonVal = folly::parseJson(readFile(valSizeDistFile));
    JSONSetVal(configJsonVal, valSizeRange);
    JSONSetVal(configJsonVal, valSizeRangeProbability);
  }

  if (!popDistFile.empty()) {
    const auto configJsonPop = folly::parseJson(readFile(popDistFile));
    JSONSetVal(configJsonPop, popularityBuckets);
    JSONSetVal(configJsonPop, popularityWeights);
  }

  checkCorrectSize<DistributionConfig, 352>();
}

ReplayGeneratorConfig::ReplayGeneratorConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, replaySerializationMode);
  JSONSetVal(configJson, relaxedSerialIntervalMs);
  JSONSetVal(configJson, numAggregationFields);
  JSONSetVal(configJson, statsPerAggField);

  if (replaySerializationMode != "strict" &&
      replaySerializationMode != "relaxed" &&
      replaySerializationMode != "none") {
    throw std::invalid_argument(folly::sformat(
        "Unsupported request serialization mode: {}", replaySerializationMode));
  }

  checkCorrectSize<ReplayGeneratorConfig, 104>();
}

ReplayGeneratorConfig::SerializeMode
ReplayGeneratorConfig::getSerializationMode() const {
  if (replaySerializationMode == "relaxed") {
    return ReplayGeneratorConfig::SerializeMode::relaxed;
  }

  if (replaySerializationMode == "none") {
    return ReplayGeneratorConfig::SerializeMode::none;
  }

  return ReplayGeneratorConfig::SerializeMode::strict;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

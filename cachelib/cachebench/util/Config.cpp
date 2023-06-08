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

#include "cachelib/cachebench/util/Config.h"

#include <folly/FileUtil.h>
#include <folly/json.h>

#include <unordered_map>

namespace facebook {
namespace cachelib {
namespace cachebench {
StressorConfig::StressorConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, generator);

  JSONSetVal(configJson, name);

  JSONSetVal(configJson, enableLookaside);
  JSONSetVal(configJson, onlySetIfMiss);
  JSONSetVal(configJson, ignoreOpCount);
  JSONSetVal(configJson, populateItem);
  JSONSetVal(configJson, samplingIntervalMs);

  JSONSetVal(configJson, checkConsistency);
  JSONSetVal(configJson, touchValue);

  JSONSetVal(configJson, numOps);
  JSONSetVal(configJson, numThreads);
  JSONSetVal(configJson, numKeys);

  JSONSetVal(configJson, opDelayBatch);
  JSONSetVal(configJson, opDelayNs);

  JSONSetVal(configJson, opRatePerSec);
  JSONSetVal(configJson, opRateBurstSize);

  JSONSetVal(configJson, opPoolDistribution);
  JSONSetVal(configJson, keyPoolDistribution);

  JSONSetVal(configJson, maxInconsistencyCount);

  JSONSetVal(configJson, traceFileName);
  JSONSetVal(configJson, traceFileNames);
  JSONSetVal(configJson, configPath);

  JSONSetVal(configJson, cachePieceSize);
  JSONSetVal(configJson, maxCachePieces);

  JSONSetVal(configJson, maxInvalidDestructorCount);

  JSONSetVal(configJson, repeatTraceReplay);
  JSONSetVal(configJson, timestampFactor);

  JSONSetVal(configJson, checkNvmCacheWarmUp);

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

  if (!traceFileName.empty() && !traceFileNames.empty()) {
    throw std::invalid_argument(
        folly::sformat("set only one of traceFileName or traceFileNames"));
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

CacheBenchConfig::CacheBenchConfig(
    const std::string& path,
    CacheConfigCustomizer cacheConfigCustomizer,
    StressorConfigCustomizer stressorConfigCustomizer) {
  std::string configString;
  if (!folly::readFile(path.c_str(), configString)) {
    throw std::invalid_argument(
        folly::sformat("could not read file: {}", path));
  }

  std::cout << "===JSON Config===" << std::endl;
  std::cout << configString << std::endl;

  auto configJson = folly::parseJson(folly::json::stripComments(configString));
  auto& testConfigJson = configJson["test_config"];
  if (testConfigJson.getDefault("configPath", "").empty()) {
    // strip out the file name at the end to get  the directory if the path
    // contains /  and if not, the path is present directory. Assume the file
    // does not have any escaped '/'
    auto pos = path.find_last_of('/');
    const std::string configDir =
        (pos != std::string::npos)
            ? std::string{path.begin(), path.begin() + pos}
            : ".";
    testConfigJson["configPath"] = configDir;
  }

  // if present, customize the configuration.
  auto stressorConfig = StressorConfig{testConfigJson};
  stressorConfig_ = stressorConfigCustomizer
                        ? stressorConfigCustomizer(stressorConfig)
                        : stressorConfig;
  auto cacheConfig = CacheConfig{configJson["cache_config"]};
  cacheConfig_ =
      cacheConfigCustomizer ? cacheConfigCustomizer(cacheConfig) : cacheConfig;
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
  JSONSetVal(jsonConfig, updateRatio);
  JSONSetVal(jsonConfig, couldExistRatio);

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

  checkCorrectSize<DistributionConfig, 368>();
}

ReplayGeneratorConfig::ReplayGeneratorConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, ampFactor);
  JSONSetVal(configJson, replaySerializationMode);
  JSONSetVal(configJson, relaxedSerialIntervalMs);
  JSONSetVal(configJson, numAggregationFields);
  JSONSetVal(configJson, numExtraFields);
  JSONSetVal(configJson, statsPerAggField);

  if (configJson.count("mlAdmissionConfig")) {
    mlAdmissionConfig =
        std::make_shared<MLAdmissionConfig>(configJson["mlAdmissionConfig"]);
  }

  if (replaySerializationMode != "strict" &&
      replaySerializationMode != "relaxed" &&
      replaySerializationMode != "none") {
    throw std::invalid_argument(folly::sformat(
        "Unsupported request serialization mode: {}", replaySerializationMode));
  }

  checkCorrectSize<ReplayGeneratorConfig, 128>();
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

MLAdmissionConfig::MLAdmissionConfig(const folly::dynamic& configJson) {
  JSONSetVal(configJson, modelPath);
  JSONSetVal(configJson, numericFeatures);
  JSONSetVal(configJson, categoricalFeatures);
  JSONSetVal(configJson, targetRecall);
  JSONSetVal(configJson, admitCategory);

  checkCorrectSize<MLAdmissionConfig, 160>();
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

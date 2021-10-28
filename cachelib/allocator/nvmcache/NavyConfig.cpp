/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "cachelib/allocator/nvmcache/NavyConfig.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "folly/container/Access.h"

namespace facebook {
namespace cachelib {
namespace navy {
const std::string& NavyConfig::getFileName() const {
  XDCHECK(usesSimpleFile());
  return fileName_;
}
const std::vector<std::string>& NavyConfig::getRaidPaths() const {
  XDCHECK(usesRaidFiles());
  return raidPaths_;
}

// admission policy settings
RandomAPConfig& NavyConfig::enableRandomAdmPolicy() {
  if (!admissionPolicy_.empty()) {
    throw std::invalid_argument(folly::sformat(
        "{} admission policy is already enabled", admissionPolicy_));
  }
  admissionPolicy_ = "random";
  return randomAPConfig_;
}

DynamicRandomAPConfig& NavyConfig::enableDynamicRandomAdmPolicy() {
  if (!admissionPolicy_.empty()) {
    throw std::invalid_argument(folly::sformat(
        "{} admission policy is already enabled", admissionPolicy_));
  }
  admissionPolicy_ = "dynamic_random";
  return dynamicRandomAPConfig_;
}

RandomAPConfig& RandomAPConfig::setAdmProbability(double admProbability) {
  if (admProbability < 0 || admProbability > 1) {
    throw std::invalid_argument(folly::sformat(
        "admission probability should be in the range of [0, 1], but {} is set",
        admProbability));
  }
  admProbability_ = admProbability;
  return *this;
}

void NavyConfig::setAdmissionPolicy(const std::string& admissionPolicy) {
  if (admissionPolicy == "") {
    throw std::invalid_argument("admission policy should not be empty");
  }
  admissionPolicy_ = admissionPolicy;
}

void NavyConfig::setAdmissionProbability(double admissionProbability) {
  if (admissionPolicy_ != kAdmPolicyRandom) {
    throw std::invalid_argument(
        folly::sformat("admission probability is only for random policy, but "
                       "{} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  randomAPConfig_.setAdmProbability(admissionProbability);
}

void NavyConfig::setAdmissionWriteRate(uint64_t admissionWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  dynamicRandomAPConfig_.setAdmWriteRate(admissionWriteRate);
}

void NavyConfig::setMaxWriteRate(uint64_t maxWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("max write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  dynamicRandomAPConfig_.setMaxWriteRate(maxWriteRate);
}

void NavyConfig::setAdmissionSuffixLength(size_t admissionSuffixLen) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission suffix length is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  dynamicRandomAPConfig_.setAdmSuffixLength(admissionSuffixLen);
}

void NavyConfig::setAdmissionProbBaseSize(uint32_t admissionProbBaseSize) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission probability base size is only for "
                       "dynamic_random policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  dynamicRandomAPConfig_.setAdmProbBaseSize(admissionProbBaseSize);
}

// file settings
void NavyConfig::setSimpleFile(const std::string& fileName,
                               uint64_t fileSize,
                               bool truncateFile) {
  if (usesRaidFiles()) {
    throw std::invalid_argument("already set RAID files");
  }
  fileName_ = fileName;
  fileSize_ = fileSize;
  truncateFile_ = truncateFile;
}

void NavyConfig::setRaidFiles(std::vector<std::string> raidPaths,
                              uint64_t fileSize,
                              bool truncateFile) {
  if (usesSimpleFile()) {
    throw std::invalid_argument("already set a simple file");
  }
  if (raidPaths.size() <= 1) {
    throw std::invalid_argument(folly::sformat(
        "RAID needs at least two paths, but {} path is set", raidPaths.size()));
  }
  raidPaths_ = std::move(raidPaths);
  fileSize_ = fileSize;
  truncateFile_ = truncateFile;
}

BlockCacheConfig& BlockCacheConfig::enableHitsBasedReinsertion(
    uint8_t hitsThreshold) {
  reinsertionConfig_.enableHitsBased(hitsThreshold);
  return *this;
}

BlockCacheConfig& BlockCacheConfig::enablePctBasedReinsertion(
    unsigned int pctThreshold) {
  reinsertionConfig_.enablePctBased(pctThreshold);
  return *this;
}

BlockCacheConfig& BlockCacheConfig::setCleanRegions(
    uint32_t cleanRegions, bool enableInMemBuffer) noexcept {
  cleanRegions_ = cleanRegions;
  if (enableInMemBuffer) {
    // Increasing number of in-mem buffers is a short-term mitigation
    // to avoid reinsertion failure when all buffers in clean regions
    // are pending flush and the reclaim job is running before flushing complete
    // (see T93961857, T93959811)
    numInMemBuffers_ = 2 * cleanRegions;
  }
  return *this;
}

void NavyConfig::setBlockCacheLru(bool blockCacheLru) {
  if (!blockCacheLru) {
    blockCacheConfig_.enableFifo();
  }
}

void NavyConfig::setBlockCacheSegmentedFifoSegmentRatio(
    std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio) {
  blockCacheConfig_.enableSegmentedFifo(blockCacheSegmentedFifoSegmentRatio);
}

void NavyConfig::setBlockCacheReinsertionHitsThreshold(
    uint8_t blockCacheReinsertionHitsThreshold) {
  blockCacheConfig_.reinsertionConfig_.enableHitsBased(
      blockCacheReinsertionHitsThreshold);
}

void NavyConfig::setBlockCacheReinsertionProbabilityThreshold(
    unsigned int blockCacheReinsertionProbabilityThreshold) {
  blockCacheConfig_.reinsertionConfig_.enablePctBased(
      blockCacheReinsertionProbabilityThreshold);
}

// BigHash settings
BigHashConfig& BigHashConfig::setSizePctAndMaxItemSize(
    unsigned int sizePct, uint64_t smallItemMaxSize) {
  if (sizePct > 100) {
    throw std::invalid_argument(folly::sformat(
        "to enable BigHash, BigHash size pct should be in the range of [0, 100]"
        ", but {} is set",
        sizePct));
  }
  if (sizePct == 0) {
    XLOG(INFO) << "BigHash is not configured";
  }
  sizePct_ = sizePct;
  smallItemMaxSize_ = smallItemMaxSize;
  return *this;
}

void NavyConfig::setBigHash(unsigned int bigHashSizePct,
                            uint32_t bigHashBucketSize,
                            uint64_t bigHashBucketBfSize,
                            uint64_t bigHashSmallItemMaxSize) {
  bigHashConfig_
      .setSizePctAndMaxItemSize(bigHashSizePct, bigHashSmallItemMaxSize)
      .setBucketSize(bigHashBucketSize)
      .setBucketBfSize(bigHashBucketBfSize);
}
// job scheduler settings
void NavyConfig::setNavyReqOrderingShards(uint64_t navyReqOrderingShards) {
  if (navyReqOrderingShards == 0) {
    throw std::invalid_argument(
        "Navy request ordering shards should always be non-zero");
  }
  navyReqOrderingShards_ = navyReqOrderingShards;
}

std::map<std::string, std::string> NavyConfig::serialize() const {
  auto configMap = std::map<std::string, std::string>();

  // admission policy settings
  configMap["navyConfig::admissionPolicy"] = getAdmissionPolicy();
  configMap["navyConfig::admissionProbability"] =
      folly::to<std::string>(randomAPConfig_.getAdmProbability());
  configMap["navyConfig::admissionWriteRate"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getAdmWriteRate());
  configMap["navyConfig::maxWriteRate"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getMaxWriteRate());
  configMap["navyConfig::admissionSuffixLen"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getAdmSuffixLength());
  configMap["navyConfig::admissionProbBaseSize"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getAdmProbBaseSize());
  configMap["navyConfig::admissionProbFactorLowerBound"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getProbFactorLowerBound());
  configMap["navyConfig::admissionProbFactorUpperBound"] =
      folly::to<std::string>(dynamicRandomAPConfig_.getProbFactorUpperBound());

  // device settings
  configMap["navyConfig::blockSize"] = folly::to<std::string>(blockSize_);
  configMap["navyConfig::fileName"] = fileName_;
  configMap["navyConfig::raidPaths"] = folly::join(",", raidPaths_);
  configMap["navyConfig::deviceMetadataSize"] =
      std::to_string(deviceMetadataSize_);
  configMap["navyConfig::fileSize"] = folly::to<std::string>(fileSize_);
  configMap["navyConfig::truncateFile"] = truncateFile_ ? "true" : "false";
  configMap["navyConfig::deviceMaxWriteSize"] =
      folly::to<std::string>(deviceMaxWriteSize_);

  // BlockCache settings
  configMap["navyConfig::blockCacheLru"] =
      blockCacheConfig_.isLruEnabled() ? "true" : "false";
  configMap["navyConfig::blockCacheRegionSize"] =
      folly::to<std::string>(blockCacheConfig_.getRegionSize());
  configMap["navyConfig::blockCacheSizeClasses"] =
      folly::join(",", blockCacheConfig_.getSizeClasses());
  configMap["navyConfig::blockCacheCleanRegions"] =
      folly::to<std::string>(blockCacheConfig_.getCleanRegions());
  configMap["navyConfig::blockCacheReinsertionHitsThreshold"] =
      folly::to<std::string>(
          blockCacheConfig_.getReinsertionConfig().getHitsThreshold());
  configMap["navyConfig::blockCacheReinsertionPctThreshold"] =
      folly::to<std::string>(
          blockCacheConfig_.getReinsertionConfig().getPctThreshold());
  configMap["navyConfig::blockCacheNumInMemBuffers"] =
      folly::to<std::string>(blockCacheConfig_.getNumInMemBuffers());
  configMap["navyConfig::blockCacheDataChecksum"] =
      blockCacheConfig_.getDataChecksum() ? "true" : "false";
  configMap["navyConfig::blockCacheSegmentedFifoSegmentRatio"] =
      folly::join(",", blockCacheConfig_.getSFifoSegmentRatio());

  // BigHash settings
  configMap["navyConfig::bigHashSizePct"] =
      folly::to<std::string>(bigHashConfig_.getSizePct());
  configMap["navyConfig::bigHashBucketSize"] =
      folly::to<std::string>(bigHashConfig_.getBucketSize());
  configMap["navyConfig::bigHashBucketBfSize"] =
      folly::to<std::string>(bigHashConfig_.getBucketBfSize());
  configMap["navyConfig::bigHashSmallItemMaxSize"] =
      folly::to<std::string>(bigHashConfig_.getSmallItemMaxSize());

  // Job scheduler settings
  configMap["navyConfig::readerThreads"] =
      folly::to<std::string>(readerThreads_);
  configMap["navyConfig::writerThreads"] =
      folly::to<std::string>(writerThreads_);
  configMap["navyConfig::navyReqOrderingShards"] =
      folly::to<std::string>(navyReqOrderingShards_);

  // Other settings
  configMap["navyConfig::maxConcurrentInserts"] =
      folly::to<std::string>(maxConcurrentInserts_);
  configMap["navyConfig::maxParcelMemoryMB"] =
      folly::to<std::string>(maxParcelMemoryMB_);
  return configMap;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

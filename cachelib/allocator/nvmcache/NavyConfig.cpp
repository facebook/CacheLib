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
void NavyConfig::setAdmissionPolicy(const std::string& admissionPolicy) {
  if (admissionPolicy == "") {
    throw std::invalid_argument("admission policy should not be empty");
  }

  admissionPolicy_ = admissionPolicy;
  enabled_ = true;
}

void NavyConfig::setAdmissionProbability(double admissionProbability) {
  if (admissionProbability < 0 || admissionProbability > 1) {
    throw std::invalid_argument(folly::sformat(
        "admission probability should between 0 and 1, but {} is set",
        admissionProbability));
  }

  if (admissionPolicy_ != kAdmPolicyRandom) {
    throw std::invalid_argument(
        folly::sformat("admission probability is only for random policy, but "
                       "{} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionProbability_ = admissionProbability;
  enabled_ = true;
}

void NavyConfig::setAdmissionWriteRate(uint64_t admissionWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionWriteRate_ = admissionWriteRate;
  enabled_ = true;
}

void NavyConfig::setMaxWriteRate(uint64_t maxWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("max write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  maxWriteRate_ = maxWriteRate;
  enabled_ = true;
}

void NavyConfig::setAdmissionSuffixLength(size_t admissionSuffixLen) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission suffix length is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionSuffixLen_ = admissionSuffixLen;
  enabled_ = true;
}

void NavyConfig::setAdmissionProbBaseSize(uint32_t admissionProbBaseSize) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission probability base size is only for "
                       "dynamic_random policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionProbBaseSize_ = admissionProbBaseSize;
  enabled_ = true;
}

void NavyConfig::setNavyReqOrderingShards(uint64_t navyReqOrderingShards) {
  if (navyReqOrderingShards > 0) {
    navyReqOrderingShards_ = navyReqOrderingShards;
    enabled_ = true;
  } else {
    throw std::invalid_argument(
        "Navy request ordering shards should always be non-zero");
  }
}

std::map<std::string, std::string> NavyConfig::serialize() const {
  auto configMap = std::map<std::string, std::string>();

  // admission policy settings
  configMap["navyConfig::admissionPolicy"] = admissionPolicy_;
  configMap["navyConfig::admissionProbability"] =
      folly::to<std::string>(admissionProbability_);
  configMap["navyConfig::admissionWriteRate"] =
      folly::to<std::string>(admissionWriteRate_);
  configMap["navyConfig::maxWriteRate"] = folly::to<std::string>(maxWriteRate_);
  configMap["navyConfig::admissionSuffixLen"] =
      folly::to<std::string>(admissionSuffixLen_);
  configMap["navyConfig::admissionProbBaseSize"] =
      folly::to<std::string>(admissionProbBaseSize_);

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
  configMap["navyConfig::blockCacheLru"] = blockCacheLru_ ? "true" : "false";
  configMap["navyConfig::blockCacheRegionSize"] =
      folly::to<std::string>(blockCacheRegionSize_);
  configMap["navyConfig::blockCacheReadBufferSize"] =
      folly::to<std::string>(blockCacheReadBufferSize_);
  configMap["navyConfig::blockCacheSizeClasses"] =
      folly::join(",", blockCacheSizeClasses_);
  configMap["navyConfig::blockCacheCleanRegions"] =
      folly::to<std::string>(blockCacheCleanRegions_);
  configMap["navyConfig::blockCacheReinsertionHitsThreshold"] =
      folly::to<std::string>(blockCacheReinsertionHitsThreshold_);
  configMap["navyConfig::blockCacheReinsertionProbabilityThreshold"] =
      folly::to<std::string>(blockCacheReinsertionProbabilityThreshold_);
  configMap["navyConfig::blockCacheNumInMemBuffers"] =
      folly::to<std::string>(blockCacheNumInMemBuffers_);
  configMap["navyConfig::blockCacheDataChecksum"] =
      blockCacheDataChecksum_ ? "true" : "false";
  configMap["navyConfig::blockCacheSegmentedFifoSegmentRatio"] =
      folly::join(",", blockCacheSegmentedFifoSegmentRatio_);

  // BigHash settings
  configMap["navyConfig::bigHashSizePct"] =
      folly::to<std::string>(bigHashSizePct_);
  configMap["navyConfig::bigHashBucketSize"] =
      folly::to<std::string>(bigHashBucketSize_);
  configMap["navyConfig::bigHashBucketBfSize"] =
      folly::to<std::string>(bigHashBucketBfSize_);
  configMap["navyConfig::bigHashSmallItemMaxSize"] =
      folly::to<std::string>(bigHashSmallItemMaxSize_);

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

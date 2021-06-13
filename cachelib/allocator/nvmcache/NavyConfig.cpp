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
}

void NavyConfig::setAdmissionWriteRate(uint64_t admissionWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionWriteRate_ = admissionWriteRate;
}

void NavyConfig::setMaxWriteRate(uint64_t maxWriteRate) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("max write rate is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  maxWriteRate_ = maxWriteRate;
}

void NavyConfig::setAdmissionSuffixLength(size_t admissionSuffixLen) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission suffix length is only for dynamic_random "
                       "policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionSuffixLen_ = admissionSuffixLen;
}

void NavyConfig::setAdmissionProbBaseSize(uint32_t admissionProbBaseSize) {
  if (admissionPolicy_ != kAdmPolicyDynamicRandom) {
    throw std::invalid_argument(
        folly::sformat("admission probability base size is only for "
                       "dynamic_random policy, but {} policy was set",
                       admissionPolicy_.empty() ? "no" : admissionPolicy_));
  }
  admissionProbBaseSize_ = admissionProbBaseSize;
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

// BlockCache settings
void NavyConfig::setBlockCacheLru(bool blockCacheLru) {
  if (blockCacheLru && !blockCacheSegmentedFifoSegmentRatio_.empty()) {
    throw std::invalid_argument(
        "already set sfifo segment ratio, should not use LRU policy");
  }
  blockCacheLru_ = blockCacheLru;
}

void NavyConfig::setBlockCacheSegmentedFifoSegmentRatio(
    std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio) {
  if (blockCacheLru_) {
    throw std::invalid_argument(
        "already use LRU policy, should not set sfifo segment ratio");
  }
  blockCacheSegmentedFifoSegmentRatio_ =
      std::move(blockCacheSegmentedFifoSegmentRatio);
}

void NavyConfig::setBlockCacheReinsertionHitsThreshold(
    uint8_t blockCacheReinsertionHitsThreshold) {
  if (blockCacheReinsertionProbabilityThreshold_ > 0) {
    throw std::invalid_argument(
        "already set reinsertion probability threshold, should not set "
        "reinsertion hits threshold");
  }
  blockCacheReinsertionHitsThreshold_ = blockCacheReinsertionHitsThreshold;
}

void NavyConfig::setBlockCacheReinsertionProbabilityThreshold(
    unsigned int blockCacheReinsertionProbabilityThreshold) {
  if (blockCacheReinsertionHitsThreshold_ > 0) {
    throw std::invalid_argument(
        "already set reinsertion hits threshold, should not set reinsertion "
        "probability threshold");
  }
  if (blockCacheReinsertionProbabilityThreshold > 100) {
    throw std::invalid_argument(
        folly::sformat("reinsertion probability threshold should between 0 and "
                       "100, but {} is set",
                       blockCacheReinsertionProbabilityThreshold));
  }
  blockCacheReinsertionProbabilityThreshold_ =
      blockCacheReinsertionProbabilityThreshold;
}

// BigHash settings
NavyConfig::BigHashConfig& NavyConfig::BigHashConfig::setSizePctAndMaxItemSize(
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
      folly::to<std::string>(getBigHashSizePct());
  configMap["navyConfig::bigHashBucketSize"] =
      folly::to<std::string>(getBigHashBucketSize());
  configMap["navyConfig::bigHashBucketBfSize"] =
      folly::to<std::string>(getBigHashBucketBfSize());
  configMap["navyConfig::bigHashSmallItemMaxSize"] =
      folly::to<std::string>(getBigHashSmallItemMaxSize());

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

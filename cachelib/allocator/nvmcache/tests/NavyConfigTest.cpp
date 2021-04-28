#include <gtest/gtest.h>

#include <sstream>
#include <stdexcept>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/common/Types.h"
namespace facebook {
namespace cachelib {
namespace tests {
using NavyConfig = navy::NavyConfig;
namespace {
// AP settings
const std::string admissionPolicy = "dynamic_random";
const uint64_t admissionWriteRate = 100;
const uint64_t maxWriteRate = 160;
const size_t admissionSuffixLen = 1;
const uint64_t admissionProbBaseSize = 1024;

// device settings
const uint64_t blockSize = 1024;
const std::string fileName = "test";
const std::vector<std::string> raidPaths = {"test1", "test2"};
const std::vector<std::string> raidPathsInvalid = {"test1"};
const uint64_t deviceMetadataSize = 1024 * 1024 * 1024;
const uint64_t fileSize = 10 * 1024 * 1024;
const bool truncateFile = false;
const uint32_t deviceMaxWriteSize = 4 * 1024 * 1024;

// BlockCache settings
const uint32_t blockCacheRegionSize = 16 * 1024 * 1024;
const uint64_t blockCacheReadBufferSize = 1024;
const std::vector<uint32_t> blockCacheSizeClasses = {1024, 2048, 4096};
const uint32_t blockCacheCleanRegions = 4;
const uint8_t blockCacheReinsertionHitsThreshold = 111;
const uint32_t blockCacheNumInMemBuffers = 8;
const bool blockCacheDataChecksum = true;
const std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio = {111, 222,
                                                                       333};
// BigCache settings
const unsigned int bigHashSizePct = 50;
const uint32_t bigHashBucketSize = 1024;
const uint64_t bigHashBucketBfSize = 4;
const uint64_t bigHashSmallItemMaxSize = 512;

const uint32_t maxConcurrentInserts = 50000;
const uint64_t maxParcelMemoryMB = 512;

// Job scheduler settings
const unsigned int readerThreads = 40;
const unsigned int writerThreads = 40;
const uint64_t navyReqOrderingShards = 30;

void setAdmissionPolicyTestSettings(NavyConfig& config) {
  config.setAdmissionPolicy(admissionPolicy);
  config.setAdmissionWriteRate(admissionWriteRate);
  config.setMaxWriteRate(maxWriteRate);
  config.setAdmissionSuffixLength(admissionSuffixLen);
  config.setAdmissionProbBaseSize(admissionProbBaseSize);
}

void setDeviceTestSettings(NavyConfig& config) {
  config.setBlockSize(blockSize);
  config.setRaidFiles(raidPaths, fileSize, truncateFile);
  config.setDeviceMetadataSize(deviceMetadataSize);
  config.setDeviceMaxWriteSize(deviceMaxWriteSize);
}

void setBlockCacheTestSettings(NavyConfig& config) {
  config.setBlockCacheLru(false);
  config.setBlockCacheRegionSize(blockCacheRegionSize);
  config.setBlockCacheSizeClasses(blockCacheSizeClasses);
  config.setBlockCacheCleanRegions(blockCacheCleanRegions);
  config.setBlockCacheReinsertionHitsThreshold(
      blockCacheReinsertionHitsThreshold);
  config.setBlockCacheNumInMemBuffers(blockCacheNumInMemBuffers);
  config.setBlockCacheDataChecksum(blockCacheDataChecksum);
  config.setBlockCacheSegmentedFifoSegmentRatio(
      blockCacheSegmentedFifoSegmentRatio);
}

void setBigHashTestSettings(NavyConfig& config) {
  config.setBigHash(bigHashSizePct,
                    bigHashBucketSize,
                    bigHashBucketBfSize,
                    bigHashSmallItemMaxSize);
}

void setJobSchedulerTestSettings(NavyConfig& config) {
  config.setReaderAndWriterThreads(readerThreads, writerThreads);
  config.setNavyReqOrderingShards(navyReqOrderingShards);
}

void setTestNavyConfig(NavyConfig& config) {
  setAdmissionPolicyTestSettings(config);
  setDeviceTestSettings(config);
  setBlockCacheTestSettings(config);
  setBigHashTestSettings(config);
  setJobSchedulerTestSettings(config);
  config.setMaxConcurrentInserts(maxConcurrentInserts);
  config.setMaxParcelMemoryMB(maxParcelMemoryMB);
}
} // namespace
TEST(NavyConfigTest, DefaultVal) {
  NavyConfig config{};
  EXPECT_EQ(config.getAdmissionPolicy(), "");
  EXPECT_EQ(config.getAdmissionSuffixLength(), 0);
  EXPECT_EQ(config.getBlockSize(), 4096);
  EXPECT_EQ(config.getTruncateFile(), false);

  EXPECT_EQ(config.getBlockCacheLru(), true);
  EXPECT_EQ(config.getBlockCacheRegionSize(), 16 * 1024 * 1024);
  EXPECT_EQ(config.getBlockCacheReadBufferSize(), 4096);
  EXPECT_EQ(config.getBlockCacheCleanRegions(), 1);
  EXPECT_TRUE(config.getBlockCacheSizeClasses().empty());
  EXPECT_TRUE(config.getBlockCacheSegmentedFifoSegmentRatio().empty());
  EXPECT_EQ(config.getBlockCacheDataChecksum(), true);

  EXPECT_EQ(config.getBigHashBucketSize(), 4096);
  EXPECT_EQ(config.getBigHashBucketBfSize(), 8);

  EXPECT_EQ(config.getMaxConcurrentInserts(), 1'000'000);
  EXPECT_EQ(config.getMaxParcelMemoryMB(), 256);

  EXPECT_EQ(config.getReaderThreads(), 32);
  EXPECT_EQ(config.getWriterThreads(), 32);
  EXPECT_EQ(config.getNavyReqOrderingShards(), 20);

  EXPECT_EQ(config.getAdmissionProbability(), 0);
  EXPECT_EQ(config.getAdmissionWriteRate(), 0);
  EXPECT_EQ(config.getDeviceMetadataSize(), 0);
  EXPECT_EQ(config.getFileSize(), 0);
  EXPECT_EQ(config.getDeviceMaxWriteSize(), 0);
  EXPECT_EQ(config.getBlockCacheNumInMemBuffers(), 0);
  EXPECT_EQ(config.getBigHashSmallItemMaxSize(), 0);

  EXPECT_EQ(config.usesSimpleFile(), false);
  EXPECT_EQ(config.usesRaidFiles(), false);

  EXPECT_FALSE(config.isEnabled());
}

TEST(NavyConfigTest, Serialization) {
  NavyConfig config{};
  setTestNavyConfig(config);
  std::map<std::string, std::string> configMap = config.serialize();

  auto expectedConfigMap = std::map<std::string, std::string>();
  expectedConfigMap["navyConfig::admissionPolicy"] = "dynamic_random";
  expectedConfigMap["navyConfig::admissionProbability"] = "0";
  expectedConfigMap["navyConfig::admissionWriteRate"] = "100";
  expectedConfigMap["navyConfig::maxWriteRate"] = "160";
  expectedConfigMap["navyConfig::admissionSuffixLen"] = "1";
  expectedConfigMap["navyConfig::admissionProbBaseSize"] = "1024";

  expectedConfigMap["navyConfig::blockSize"] = "1024";
  expectedConfigMap["navyConfig::fileName"] = "";
  expectedConfigMap["navyConfig::raidPaths"] = "test1,test2";
  expectedConfigMap["navyConfig::deviceMetadataSize"] = "1073741824";
  expectedConfigMap["navyConfig::fileSize"] = "10485760";
  expectedConfigMap["navyConfig::truncateFile"] = "false";
  expectedConfigMap["navyConfig::deviceMaxWriteSize"] = "4194304";

  expectedConfigMap["navyConfig::blockCacheLru"] = "false";
  expectedConfigMap["navyConfig::blockCacheRegionSize"] = "16777216";
  expectedConfigMap["navyConfig::blockCacheReadBufferSize"] = "4096";
  expectedConfigMap["navyConfig::blockCacheSizeClasses"] = "1024,2048,4096";
  expectedConfigMap["navyConfig::blockCacheCleanRegions"] = "4";
  expectedConfigMap["navyConfig::blockCacheReinsertionHitsThreshold"] = "111";
  expectedConfigMap["navyConfig::blockCacheReinsertionProbabilityThreshold"] =
      "0";
  expectedConfigMap["navyConfig::blockCacheNumInMemBuffers"] = "8";
  expectedConfigMap["navyConfig::blockCacheDataChecksum"] = "true";
  expectedConfigMap["navyConfig::blockCacheSegmentedFifoSegmentRatio"] =
      "111,222,333";

  expectedConfigMap["navyConfig::bigHashSizePct"] = "50";
  expectedConfigMap["navyConfig::bigHashBucketSize"] = "1024";
  expectedConfigMap["navyConfig::bigHashBucketBfSize"] = "4";
  expectedConfigMap["navyConfig::bigHashSmallItemMaxSize"] = "512";

  expectedConfigMap["navyConfig::maxConcurrentInserts"] = "50000";
  expectedConfigMap["navyConfig::maxParcelMemoryMB"] = "512";

  expectedConfigMap["navyConfig::readerThreads"] = "40";
  expectedConfigMap["navyConfig::writerThreads"] = "40";
  expectedConfigMap["navyConfig::navyReqOrderingShards"] = "30";

  EXPECT_EQ(configMap, expectedConfigMap);
}

TEST(NavyConfigTest, AdmissionPolicy) {
  // set random admission policy
  NavyConfig config1{};
  EXPECT_THROW(config1.setAdmissionPolicy(""), std::invalid_argument);
  EXPECT_NO_THROW(config1.setAdmissionPolicy("random"));
  EXPECT_THROW(config1.setAdmissionProbability(2), std::invalid_argument);
  EXPECT_NO_THROW(config1.setAdmissionProbability(0.5));
  EXPECT_EQ(config1.getAdmissionPolicy(), NavyConfig::kAdmPolicyRandom);
  EXPECT_EQ(config1.getAdmissionProbability(), 0.5);
  // cannot set dynamic_random parameters
  EXPECT_THROW(config1.setAdmissionWriteRate(admissionWriteRate),
               std::invalid_argument);
  EXPECT_THROW(config1.setMaxWriteRate(maxWriteRate), std::invalid_argument);
  EXPECT_THROW(config1.setAdmissionSuffixLength(admissionSuffixLen),
               std::invalid_argument);
  EXPECT_THROW(config1.setAdmissionProbBaseSize(admissionProbBaseSize),
               std::invalid_argument);

  // set dynamic_random policy
  NavyConfig config2{};
  EXPECT_NO_THROW(config2.setAdmissionPolicy("dynamic_random"));
  EXPECT_NO_THROW(config2.setAdmissionWriteRate(admissionWriteRate));
  EXPECT_NO_THROW(config2.setMaxWriteRate(maxWriteRate));
  EXPECT_NO_THROW(config2.setAdmissionSuffixLength(admissionSuffixLen));
  EXPECT_NO_THROW(config2.setAdmissionProbBaseSize(admissionProbBaseSize));
  EXPECT_EQ(config2.getAdmissionPolicy(), NavyConfig::kAdmPolicyDynamicRandom);
  EXPECT_EQ(config2.getAdmissionWriteRate(), admissionWriteRate);
  EXPECT_EQ(config2.getMaxWriteRate(), maxWriteRate);
  EXPECT_EQ(config2.getAdmissionSuffixLength(), admissionSuffixLen);
  EXPECT_EQ(config2.getAdmissionProbBaseSize(), admissionProbBaseSize);
  // cannot set random parameters
  EXPECT_THROW(config2.setAdmissionProbability(0.5), std::invalid_argument);

  EXPECT_TRUE(config1.isEnabled());
  EXPECT_TRUE(config2.isEnabled());
}

TEST(NavyConfigTest, Device) {
  NavyConfig config1{};
  config1.setBlockSize(blockSize);
  config1.setDeviceMetadataSize(deviceMetadataSize);
  EXPECT_EQ(config1.getBlockSize(), blockSize);
  EXPECT_EQ(config1.getDeviceMetadataSize(), deviceMetadataSize);

  // set simple file
  config1.setSimpleFile(fileName, fileSize, truncateFile);
  EXPECT_EQ(config1.getFileName(), fileName);
  EXPECT_EQ(config1.getFileSize(), fileSize);
  EXPECT_EQ(config1.getTruncateFile(), truncateFile);
  EXPECT_THROW(config1.setRaidFiles(raidPaths, fileSize, truncateFile),
               std::invalid_argument);

  // set RAID files
  NavyConfig config2{};
  EXPECT_THROW(config2.setRaidFiles(raidPathsInvalid, fileSize, truncateFile),
               std::invalid_argument);
  config2.setRaidFiles(raidPaths, fileSize, truncateFile);
  EXPECT_EQ(config2.getRaidPaths(), raidPaths);
  EXPECT_EQ(config2.getFileSize(), fileSize);
  EXPECT_EQ(config1.getTruncateFile(), truncateFile);
  EXPECT_THROW(config2.setSimpleFile(fileName, fileSize, truncateFile),
               std::invalid_argument);

  EXPECT_TRUE(config1.isEnabled());
  EXPECT_TRUE(config2.isEnabled());
}

TEST(NavyConfigTest, BlockCache) {
  NavyConfig config0{};
  config0.setBlockCacheRegionSize(blockCacheRegionSize);
  config0.setBlockCacheCleanRegions(blockCacheCleanRegions);
  config0.setBlockCacheNumInMemBuffers(blockCacheNumInMemBuffers);
  config0.setBlockCacheDataChecksum(blockCacheDataChecksum);

  EXPECT_EQ(config0.getBlockCacheRegionSize(), blockCacheRegionSize);
  EXPECT_EQ(config0.getBlockCacheCleanRegions(), blockCacheCleanRegions);
  EXPECT_EQ(config0.getBlockCacheNumInMemBuffers(), blockCacheNumInMemBuffers);
  EXPECT_EQ(config0.getBlockCacheDataChecksum(), blockCacheDataChecksum);

  // Test cannot set both LRU = true and segmentedFifoSegmentRatio
  NavyConfig config1{};
  config1.setBlockCacheLru(true);
  EXPECT_THROW(config1.setBlockCacheSegmentedFifoSegmentRatio(
                   blockCacheSegmentedFifoSegmentRatio),
               std::invalid_argument);
  EXPECT_EQ(config1.getBlockCacheLru(), true);
  EXPECT_TRUE(config1.getBlockCacheSegmentedFifoSegmentRatio().empty());

  NavyConfig config2{};
  config2.setBlockCacheLru(false);
  config2.setBlockCacheSegmentedFifoSegmentRatio(
      blockCacheSegmentedFifoSegmentRatio);
  EXPECT_THROW(config2.setBlockCacheLru(true), std::invalid_argument);
  EXPECT_EQ(config2.getBlockCacheSegmentedFifoSegmentRatio(),
            blockCacheSegmentedFifoSegmentRatio);
  EXPECT_EQ(config2.getBlockCacheLru(), false);

  // Test cannot set both size classes and read buffer
  NavyConfig config3{};
  config3.setBlockCacheSizeClasses(blockCacheSizeClasses);
  EXPECT_THROW(config3.setBlockCacheReadBufferSize(blockCacheReadBufferSize),
               std::invalid_argument);
  EXPECT_EQ(config3.getBlockCacheSizeClasses(), blockCacheSizeClasses);

  NavyConfig config4{};
  config4.setBlockCacheReadBufferSize(blockCacheReadBufferSize);
  EXPECT_THROW(config4.setBlockCacheSizeClasses(blockCacheSizeClasses),
               std::invalid_argument);
  EXPECT_EQ(config4.getBlockCacheReadBufferSize(), blockCacheReadBufferSize);
  EXPECT_TRUE(config4.getBlockCacheSizeClasses().empty());

  // Test cannot set both reinsertionProbabilityThreshold and
  // reinsertionProbabilityThreshold
  NavyConfig config5{};
  config5.setBlockCacheReinsertionHitsThreshold(
      blockCacheReinsertionHitsThreshold);
  EXPECT_THROW(config5.setBlockCacheReinsertionProbabilityThreshold(50),
               std::invalid_argument);
  EXPECT_EQ(config5.getBlockCacheReinsertionHitsThreshold(),
            blockCacheReinsertionHitsThreshold);
  EXPECT_EQ(config5.getBlockCacheReinsertionProbabilityThreshold(), 0);

  NavyConfig config6{};
  EXPECT_THROW(config6.setBlockCacheReinsertionProbabilityThreshold(200),
               std::invalid_argument);
  config6.setBlockCacheReinsertionProbabilityThreshold(50);
  EXPECT_THROW(config6.setBlockCacheReinsertionHitsThreshold(
                   blockCacheReinsertionHitsThreshold),
               std::invalid_argument);
  EXPECT_EQ(config6.getBlockCacheReinsertionProbabilityThreshold(), 50);
  EXPECT_EQ(config6.getBlockCacheReinsertionHitsThreshold(), 0);

  EXPECT_TRUE(config0.isEnabled());
  EXPECT_TRUE(config1.isEnabled());
  EXPECT_TRUE(config2.isEnabled());
  EXPECT_TRUE(config3.isEnabled());
  EXPECT_TRUE(config4.isEnabled());
  EXPECT_TRUE(config5.isEnabled());
  EXPECT_TRUE(config6.isEnabled());
}

TEST(NavyConfigTest, BigHash) {
  NavyConfig config{};
  EXPECT_THROW(
      config.setBigHash(
          200, bigHashBucketSize, bigHashBucketBfSize, bigHashSmallItemMaxSize),
      std::invalid_argument);
  config.setBigHash(bigHashSizePct,
                    bigHashBucketSize,
                    bigHashBucketBfSize,
                    bigHashSmallItemMaxSize);
  EXPECT_EQ(config.getBigHashSizePct(), bigHashSizePct);
  EXPECT_EQ(config.getBigHashBucketSize(), bigHashBucketSize);
  EXPECT_EQ(config.getBigHashBucketBfSize(), bigHashBucketBfSize);
  EXPECT_EQ(config.getBigHashSmallItemMaxSize(), bigHashSmallItemMaxSize);
  EXPECT_TRUE(config.isEnabled());
}

TEST(NavyConfigTest, JobScheduler) {
  NavyConfig config{};
  config.setReaderAndWriterThreads(readerThreads, writerThreads);
  ASSERT_THROW(config.setNavyReqOrderingShards(0), std::invalid_argument);
  config.setNavyReqOrderingShards(navyReqOrderingShards);
  EXPECT_EQ(config.getReaderThreads(), readerThreads);
  EXPECT_EQ(config.getWriterThreads(), writerThreads);
  EXPECT_EQ(config.getNavyReqOrderingShards(), navyReqOrderingShards);
  EXPECT_TRUE(config.isEnabled());
}

TEST(NavyConfigTest, OtherSettings) {
  NavyConfig config{};
  config.setMaxConcurrentInserts(maxConcurrentInserts);
  config.setMaxParcelMemoryMB(maxParcelMemoryMB);
  EXPECT_EQ(config.getMaxConcurrentInserts(), maxConcurrentInserts);
  EXPECT_EQ(config.getMaxParcelMemoryMB(), maxParcelMemoryMB);
  EXPECT_TRUE(config.isEnabled());
}
} // namespace tests
} // namespace cachelib
} // namespace facebook

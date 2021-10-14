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

#pragma once

#include <folly/dynamic.h>
#include <folly/logging/xlog.h>
namespace facebook {
namespace cachelib {
namespace navy {
/**
 * RandomAPConfig provides APIs for users to configure one of the admission
 * policy - "random". Admission policy is one part of NavyConfig.
 *
 * By this class, users can:
 * - set admission probability
 * - get the value of admission probability
 */
class RandomAPConfig {
 public:
  // Set admission probability for "random" policy.
  // @throw std::std::invalid_argument if the input value is not in the range
  //        of [0, 1].
  RandomAPConfig& setAdmProbability(double admProbability);

  double getAdmProbability() const { return admProbability_; }

 private:
  // Admission probability in decimal form.
  double admProbability_{};
};

/**
 * RandomDynamicAPConfig provides APIs for users to configure one of the
 * admission policy - "dynamic_random". Admission policy is one part of
 * NavyConfig.
 *
 *
 * By this class, users can:
 * - set admission target write rate
 * - set max write rate
 * - set admission suffix length
 * - set base size of baseProbability calculation
 * - get the values of the above parameters
 */
class DynamicRandomAPConfig {
 public:
  // Set admission policy's target rate in bytes/s.
  // This target is enforced across a window in average. Default to be 0 if not
  // set, meaning no rate limiting.
  DynamicRandomAPConfig& setAdmWriteRate(uint64_t admWriteRate) noexcept {
    admWriteRate_ = admWriteRate;
    return *this;
  }

  // Set the max write rate to device in bytes/s.
  // This ensures write at any given second don't exceed this limit despite a
  // possibility of writing more to stay within the target rate above.
  DynamicRandomAPConfig& setMaxWriteRate(uint64_t maxWriteRate) noexcept {
    maxWriteRate_ = maxWriteRate;
    return *this;
  }

  // Set the length of suffix in key to be ignored when hashing for
  // probability.
  DynamicRandomAPConfig& setAdmSuffixLength(size_t admSuffixLen) noexcept {
    admSuffixLen_ = admSuffixLen;
    return *this;
  }

  // Set the Navy item base size for base probability calculation.
  // Set this closer to the mean size of objects. The probability is scaled for
  // other sizes by using this size as the pivot.
  DynamicRandomAPConfig& setAdmProbBaseSize(uint32_t admProbBaseSize) noexcept {
    admProbBaseSize_ = admProbBaseSize;
    return *this;
  }

  // Set the range for probability factor.
  // Non-positive values in either of the field would make
  // both values ignored and the default values from DynamicRandomAP::Config
  // will be used.
  DynamicRandomAPConfig& setProbFactorRange(double lowerBound,
                                            double upperBound) noexcept {
    probFactorLowerBound_ = lowerBound;
    probFactorUpperBound_ = upperBound;
    return *this;
  }

  uint64_t getAdmWriteRate() const { return admWriteRate_; }

  uint64_t getMaxWriteRate() const { return maxWriteRate_; }

  size_t getAdmSuffixLength() const { return admSuffixLen_; }

  uint32_t getAdmProbBaseSize() const { return admProbBaseSize_; }

  double getProbFactorLowerBound() const { return probFactorLowerBound_; }

  double getProbFactorUpperBound() const { return probFactorUpperBound_; }

 private:
  // Admission policy target rate, bytes/s.
  // Zero means no rate limiting.
  uint64_t admWriteRate_{0};
  // The max write rate to device in bytes/s to stay within the device limit
  // of saturation to avoid latency increase.
  uint64_t maxWriteRate_{0};
  // Length of suffix in key to be ignored when hashing for probability.
  size_t admSuffixLen_{0};
  // Navy item base size of baseProbability calculation.
  size_t admProbBaseSize_{0};
  // Lower bound of the probability factor. Non-positive valu ewould be replaced
  // the default value from DynamicRandomAP::Config
  double probFactorLowerBound_{0};
  // Upper bound of the probability factor. Non-positive value would be
  // replaced the default value from DynamicRandomAP::Config
  double probFactorUpperBound_{0};
};
/**
 * BlockCacheConfig provides APIs for users to configure BlockCache engine,
 * which is one part of NavyConfig.
 *
 * By this class, users can:
 * - enable FIFO or segmented FIFO eviction policy (default is LRU)
 * - enable hits-based OR probability-based reinsertion policy (but not both)
 * - set number of clean regions
 * - enable in-mem buffer (once enabled, the number is 2 * clean regions)
 * - set size classes
 * - set region size
 * - set data checksum
 * - get the values of all the above parameters
 */
class BlockCacheConfig {
 public:
  // Enable FIFO eviction policy (LRU will be disabled).
  BlockCacheConfig& enableFifo() noexcept {
    lru_ = false;
    return *this;
  }

  // Enable segmented FIFO eviction policy (LRU will be disabled)
  // @param sFifoSegmentRatio maps to segments in the order from
  //        least-important to most-important.
  //        e.g. {1, 1, 1} gives equal share in each of the 3 segments;
  //             {1, 2, 3} gives the 1/6th of the items in the first segment (P0
  //             least important), 2/6th of the items in the second segment
  //             (P1), and finally 3/6th of the items in the third segment (P2).
  BlockCacheConfig& enableSegmentedFifo(
      std::vector<unsigned int> sFifoSegmentRatio) noexcept {
    sFifoSegmentRatio_ = std::move(sFifoSegmentRatio);
    lru_ = false;
    return *this;
  }

  // Enable hit-based reinsertion policy.
  // When evicting regions, items that exceed this threshold of access will be
  // preserved by reinserting them internally.
  // @throw std::invalid_argument if percentage based reinsertion policy has
  //        been enabled.
  BlockCacheConfig& enableHitsBasedReinsertion(uint8_t hitsThreshold);

  // Enable percentage based reinsertion policy.
  // This is used for testing where a certain fraction of evicted items
  // (governed by the percentage) are always reinserted.
  // @throw std::invalid_argument if hit based reinsertion policy has
  //        been enabled or the input value is not in the range of 0~100.
  BlockCacheConfig& enablePctBasedReinsertion(unsigned int pctThreshold);

  // Set number of clean regions that are maintained for incoming write and
  // whether the writes are buffered in-memory.
  // When in-memory buffers are enabled, Navy maintains sufficient buffers for
  // each clean region that is reserved. This ensures each time we obtain a new
  // in-mem buffer, we have a clean region to flush it to flash once it's ready.
  BlockCacheConfig& setCleanRegions(uint32_t cleanRegions,
                                    bool enableInMemBuffer) noexcept;

  // Set a vector of size classes.
  // If enabled, Navy will configure regions to allocate rounded up to these
  // size classes and evict regions within a size classs. A given region
  // allocates corresponding to a given size class. By default, objects will be
  // stack allocated irrespective of their size on available regions.
  BlockCacheConfig& useSizeClasses(std::vector<uint32_t> sizeClasses) noexcept {
    sizeClasses_ = std::move(sizeClasses);
    return *this;
  }

  BlockCacheConfig& setRegionSize(uint32_t regionSize) noexcept {
    regionSize_ = regionSize;
    return *this;
  }

  BlockCacheConfig& setDataChecksum(bool dataChecksum) noexcept {
    dataChecksum_ = dataChecksum;
    return *this;
  }

  bool isLruEnabled() const { return lru_; }

  const std::vector<unsigned int>& getSFifoSegmentRatio() const {
    return sFifoSegmentRatio_;
  }

  uint8_t getReinsertionHitsThreshold() const {
    return reinsertionHitsThreshold_;
  }

  unsigned int getReinsertionPctThreshold() const {
    return reinsertionPctThreshold_;
  }

  uint32_t getCleanRegions() const { return cleanRegions_; }

  uint32_t getNumInMemBuffers() const { return numInMemBuffers_; }

  const std::vector<uint32_t>& getSizeClasses() const { return sizeClasses_; }

  uint32_t getRegionSize() const { return regionSize_; }

  bool getDataChecksum() const { return dataChecksum_; }

 private:
  // Whether Navy BlockCache will use region-based LRU eviction policy.
  bool lru_{true};
  // The ratio of segments for segmented FIFO eviction policy.
  // Once segmented FIFO is enabled, lru_ will be false.
  std::vector<unsigned int> sFifoSegmentRatio_;
  // Threshold of a hits based reinsertion policy with Navy BlockCache.
  // If an item had been accessed more than that threshold, it will be
  // eligible for reinsertion.
  uint8_t reinsertionHitsThreshold_{0};
  // Threshold of a percentage based reinsertion policy with Navy BlockCache.
  // The percentage value is between 0 and 100 for reinsertion.
  unsigned int reinsertionPctThreshold_{0};
  // Buffer of clean regions to maintain for eviction.
  uint32_t cleanRegions_{1};
  // Number of Navy BlockCache in-memory buffers.
  uint32_t numInMemBuffers_{0};
  // A vector of Navy BlockCache size classes (must be multiples of
  // blockSize_).
  std::vector<uint32_t> sizeClasses_;
  // Size for a region for Navy BlockCache (must be multiple of
  // blockSize_).
  uint32_t regionSize_{16 * 1024 * 1024};
  // Whether enabling data checksum for Navy BlockCache.
  bool dataChecksum_{true};

  friend class NavyConfig;
};

/**
 * BigHashConfig provides APIs for users to configure BigHash engine, which is
 * one part of NavyConfig.
 *
 * By this class, users can:
 * - enable BigHash by setting sizePct > 0
 * - set maximum item size
 * - set bucket size
 * - set bloom filter size (0 to disable bloom filter)
 * - get the values of all the above parameters
 */
class BigHashConfig {
 public:
  // Set BigHash device percentage and maximum item size(in bytes) to enable
  // BigHash engine. Default value of sizePct and smallItemMaxSize is 0,
  // meaning BigHash is not enabled.
  // @throw std::invalid_argument if sizePct is not in the range of
  //        [0, 100].
  BigHashConfig& setSizePctAndMaxItemSize(unsigned int sizePct,
                                          uint64_t smallItemMaxSize);

  // Set the bucket size in bytes for BigHash engine.
  // Default value is 4096.
  BigHashConfig& setBucketSize(uint32_t bucketSize) noexcept {
    bucketSize_ = bucketSize;
    return *this;
  }

  // Set bloom filter size per bucket in bytes for BigHash engine.
  // 0 means bloom filter will not be applied. Default value is 8.
  BigHashConfig& setBucketBfSize(uint64_t bucketBfSize) noexcept {
    bucketBfSize_ = bucketBfSize;
    return *this;
  }

  bool isBloomFilterEnabled() const { return bucketBfSize_ > 0; }

  unsigned int getSizePct() const { return sizePct_; }

  uint32_t getBucketSize() const { return bucketSize_; }

  uint64_t getBucketBfSize() const { return bucketBfSize_; }

  uint64_t getSmallItemMaxSize() const { return smallItemMaxSize_; }

 private:
  // Percentage of how much of the device out of all is given to BigHash
  // engine in Navy, e.g. 50.
  unsigned int sizePct_{0};
  // Navy BigHash engine's bucket size (must be multiple of the minimum
  // device io block size).
  // This size determines how big each bucket is and what is the physical
  // write granularity onto the device.
  uint32_t bucketSize_{4096};
  // The bloom filter size per bucket in bytes for Navy BigHash engine
  uint64_t bucketBfSize_{8};
  // The maximum item size to put into Navy BigHash engine.
  uint64_t smallItemMaxSize_{};
};

/**
 * NavyConfig provides APIs for users to set up Navy related settings for
 * NvmCache.
 *
 * Notes: the reason why these settings cannot be directly passed to Navy
 * internal config navy/Factory.h and setup there is
 * because we have logic in "NavySetup.cpp" that translates this input config
 * into CacheProto. Therefore, we need this intermediary config in NvmCache
 * Config.
 *
 */
class NavyConfig {
 public:
  static constexpr folly::StringPiece kAdmPolicyRandom{"random"};
  static constexpr folly::StringPiece kAdmPolicyDynamicRandom{"dynamic_random"};

 public:
  bool usesSimpleFile() const noexcept { return !fileName_.empty(); }
  bool usesRaidFiles() const noexcept { return raidPaths_.size() > 0; }
  bool isBigHashEnabled() const { return bigHashConfig_.getSizePct() > 0; }
  std::map<std::string, std::string> serialize() const;

  // Getters:
  // ============ Admission Policy =============
  const std::string& getAdmissionPolicy() const { return admissionPolicy_; }

  // Get a const DynamicRandomAPConfig to read values of its parameters.
  const DynamicRandomAPConfig& dynamicRandomAdmPolicy() const {
    return dynamicRandomAPConfig_;
  }

  // Get a const RandomAPConfig to read values of its parameters.
  const RandomAPConfig& randomAdmPolicy() const { return randomAPConfig_; }

  // ============ Device settings =============
  uint64_t getBlockSize() const { return blockSize_; }
  const std::string& getFileName() const;
  const std::vector<std::string>& getRaidPaths() const;
  uint64_t getDeviceMetadataSize() const { return deviceMetadataSize_; }
  uint64_t getFileSize() const { return fileSize_; }
  bool getTruncateFile() const { return truncateFile_; }
  uint32_t getDeviceMaxWriteSize() const { return deviceMaxWriteSize_; }
  uint32_t getRaidStripeSize() const {
    return blockCacheConfig_.getRegionSize();
  }

  // ============ Engine settings =============
  // Returns the threshold of classifying an item as small item or large item
  // for Navy engine.
  uint64_t getSmallItemThreshold() const {
    if (!isBigHashEnabled()) {
      return 0;
    }
    return bigHashConfig_.getSmallItemMaxSize();
  }

  // Return a const BlockCacheConfig to read values of its parameters.
  const BigHashConfig& bigHash() const { return bigHashConfig_; }

  // Return a const BlockCacheConfig to read values of its parameters.
  const BlockCacheConfig& blockCache() const { return blockCacheConfig_; }

  // ============ Job scheduler settings =============
  unsigned int getReaderThreads() const { return readerThreads_; }
  unsigned int getWriterThreads() const { return writerThreads_; }
  uint64_t getNavyReqOrderingShards() const { return navyReqOrderingShards_; }

  // other settings
  uint32_t getMaxConcurrentInserts() const { return maxConcurrentInserts_; }
  uint64_t getMaxParcelMemoryMB() const { return maxParcelMemoryMB_; }

  // Setters:
  // ============ AP settings =============
  // Set the admission policy (e.g. "random", "dynamic_random").
  // @throw std::invalid_argument on empty string.
  [[deprecated]] void setAdmissionPolicy(const std::string& admissionPolicy);
  // Set admission probability.
  // @throw std::std::invalid_argument if the admission policy is not
  //        "random" or the input value is not in the range of 0~1.
  [[deprecated]] void setAdmissionProbability(double admissionProbability);
  // Set admission policy target rate in bytes/s.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  [[deprecated]] void setAdmissionWriteRate(uint64_t admissionWriteRate);
  // Set the max write rate to device in bytes/s.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  [[deprecated]] void setMaxWriteRate(uint64_t maxWriteRate);
  // Set the length of suffix in key to be ignored when hashing for
  // probability.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  [[deprecated]] void setAdmissionSuffixLength(size_t admissionSuffixLen);
  // Set the Navy item base size of baseProbability calculation.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  [[deprecated]] void setAdmissionProbBaseSize(uint32_t admissionProbBaseSize);
  // Enable "dynamic_random" admission policy.
  // @return DynamicRandomAPConfig (for configuration)
  // @throw  invalid_argument if admissionPolicy_ is not empty
  DynamicRandomAPConfig& enableDynamicRandomAdmPolicy();

  // Enable "random" admission policy.
  // @return RandomAPConfig (for configuration)
  // @throw invalid_argument if admissionPolicy_ is not empty
  RandomAPConfig& enableRandomAdmPolicy();

  // ============ Device settings =============
  void setBlockSize(uint64_t blockSize) noexcept { blockSize_ = blockSize; }
  // Set the parameters for a simple file.
  // @throw std::invalid_argument if RAID files have been already set.
  void setSimpleFile(const std::string& fileName,
                     uint64_t fileSize,
                     bool truncateFile = false);
  // Set the parameters for RAID files.
  // @throw std::invalid_argument if a simple file has been already set
  //        or there is only one or fewer RAID paths.
  void setRaidFiles(std::vector<std::string> raidPaths,
                    uint64_t fileSize,
                    bool truncateFile = false);
  // Set the parameter for a in-memory file.
  // This function is only for cachebench and unit tests to create
  // a MemoryDevice when no file path is set.
  void setMemoryFile(uint64_t fileSize) noexcept { fileSize_ = fileSize; }
  void setDeviceMetadataSize(uint64_t deviceMetadataSize) noexcept {
    deviceMetadataSize_ = deviceMetadataSize;
  }
  void setDeviceMaxWriteSize(uint32_t deviceMaxWriteSize) noexcept {
    deviceMaxWriteSize_ = deviceMaxWriteSize;
  }

  // ============ BlockCache settings =============
  // Set whether LRU policy will be used.
  // @throw std::invalid_argument if segmentedFifoSegmentRatio has been set and
  //        blockCacheLru = true.
  [[deprecated]] void setBlockCacheLru(bool blockCacheLru);
  // Set segmentedFifoSegmentRatio for BlockCache.
  // @throw std::invalid_argument if LRU policy is used.
  [[deprecated]] void setBlockCacheSegmentedFifoSegmentRatio(
      std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio);
  // Set reinsertionHitsThreshold for BlockCache.
  // @throw std::invalid_argument if reinsertionProbabilityThreshold has been
  //        set.
  [[deprecated]] void setBlockCacheReinsertionHitsThreshold(
      uint8_t blockCacheReinsertionHitsThreshold);
  // Set ReinsertionProbabilityThreshold for BlockCache.
  // @throw std::invalid_argument if reinsertionHitsThreshold has been set or
  //        the input value is not in the range of 0~100.
  [[deprecated]] void setBlockCacheReinsertionProbabilityThreshold(
      unsigned int blockCacheReinsertionProbabilityThreshold);
  // Set size classes vector for BlockCache.
  [[deprecated]] void setBlockCacheSizeClasses(
      std::vector<uint32_t> blockCacheSizeClasses) noexcept {
    blockCacheConfig_.useSizeClasses(std::move(blockCacheSizeClasses));
  }
  // Set region size for BlockCache.
  [[deprecated]] void setBlockCacheRegionSize(
      uint32_t blockCacheRegionSize) noexcept {
    blockCacheConfig_.setRegionSize(blockCacheRegionSize);
  }
  // Set number of clean regions for BlockCache.
  [[deprecated]] void setBlockCacheCleanRegions(
      uint32_t blockCacheCleanRegions) noexcept {
    blockCacheConfig_.cleanRegions_ = blockCacheCleanRegions;
  }
  // Set number of in-mem buffers for BlockCache.
  [[deprecated]] void setBlockCacheNumInMemBuffers(
      uint32_t blockCacheNumInMemBuffers) noexcept {
    blockCacheConfig_.numInMemBuffers_ = blockCacheNumInMemBuffers;
  }
  // Set whether enable data checksum for BlockCache.
  [[deprecated]] void setBlockCacheDataChecksum(
      bool blockCacheDataChecksum) noexcept {
    blockCacheConfig_.setDataChecksum(blockCacheDataChecksum);
  }
  // Return BlockCacheConfig for configuration.
  BlockCacheConfig& blockCache() noexcept { return blockCacheConfig_; }

  // ============ BigHash settings =============
  // Set the parameters for BigHash.
  // @throw std::invalid_argument if bigHashSizePct is not in the range of
  //        0~100.
  [[deprecated]] void setBigHash(unsigned int bigHashSizePct,
                                 uint32_t bigHashBucketSize,
                                 uint64_t bigHashBucketBfSize,
                                 uint64_t bigHashSmallItemMaxSize);
  // Return BigHashConfig for configuration.
  BigHashConfig& bigHash() noexcept { return bigHashConfig_; }

  // ============ Job scheduler settings =============
  void setReaderAndWriterThreads(unsigned int readerThreads,
                                 unsigned int writerThreads) noexcept {
    readerThreads_ = readerThreads;
    writerThreads_ = writerThreads;
  }
  // Set Navy request ordering shards (expressed as power of two).
  // @throw std::invalid_argument if the input value is 0.
  void setNavyReqOrderingShards(uint64_t navyReqOrderingShards);

  // ============ Other settings =============
  void setMaxConcurrentInserts(uint32_t maxConcurrentInserts) noexcept {
    maxConcurrentInserts_ = maxConcurrentInserts;
  }
  void setMaxParcelMemoryMB(uint64_t maxParcelMemoryMB) noexcept {
    maxParcelMemoryMB_ = maxParcelMemoryMB;
  }

 private:
  // ============ AP settings =============
  // Name of the admission policy.
  // This could only be "dynamic_random" or "random" (or empty).
  std::string admissionPolicy_{""};
  DynamicRandomAPConfig dynamicRandomAPConfig_{};
  RandomAPConfig randomAPConfig_{};

  // ============ Device settings =============
  // Navy specific device block size in bytes.
  uint64_t blockSize_{4096};
  // The file name/path for caching.
  std::string fileName_;
  // An array of Navy RAID device file paths.
  std::vector<std::string> raidPaths_;
  // The size of the metadata partition on the Navy device.
  uint64_t deviceMetadataSize_{};
  // The size of the file that Navy should use.
  // 0 means to use the whole device.
  uint64_t fileSize_{};
  // Whether ask Navy to truncate the file it uses.
  bool truncateFile_{false};
  // This controls granularity of the writes when we flush the region.
  // This is only used when in-mem buffer is enabled.
  uint32_t deviceMaxWriteSize_{};

  // ============ BlockCache settings =============
  BlockCacheConfig blockCacheConfig_{};

  // ============ BigHash settings =============
  BigHashConfig bigHashConfig_{};

  // ============ Job scheduler settings =============
  // Number of asynchronous worker thread for read operation.
  unsigned int readerThreads_{32};
  // Number of asynchronous worker thread for write operation.
  unsigned int writerThreads_{32};
  // Number of shards expressed as power of two for native request ordering in
  // Navy.
  // This value needs to be non-zero.
  uint64_t navyReqOrderingShards_{20};

  // ============ Other settings =============
  // Maximum number of concurrent inserts we allow globally for Navy.
  // 0 means unlimited.
  uint32_t maxConcurrentInserts_{1'000'000};
  // Total memory limit for in-flight parcels.
  // Once this is reached, requests will be rejected until the parcel
  // memory usage gets under the limit.
  uint64_t maxParcelMemoryMB_{256};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

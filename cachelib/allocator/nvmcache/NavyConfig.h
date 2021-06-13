#pragma once

#include <folly/dynamic.h>
#include <folly/logging/xlog.h>
namespace facebook {
namespace cachelib {
namespace navy {

/**
 * NavyConfig provides APIs for users to set up Navy related settings for
 * NvmCache.
 *
 * Notes: the reason why these settings cannot be directly passed to Navy
 * internal config https://fburl.com/diffusion/y5fqozuy and setup there is
 * because we have logic in "NavySetup.cpp" that translates this input config
 * into CacheProto. Therefore, we need this intermediary config in NvmCache
 * Config.
 *
 */
class NavyConfig {
  class BigHashConfig {
   public:
    // ============ setters =============
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
    // ============ getters =============
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

 public:
  static constexpr folly::StringPiece kAdmPolicyRandom{"random"};
  static constexpr folly::StringPiece kAdmPolicyDynamicRandom{"dynamic_random"};

 public:
  bool usesSimpleFile() const noexcept { return !fileName_.empty(); }
  bool usesRaidFiles() const noexcept { return raidPaths_.size() > 0; }
  std::map<std::string, std::string> serialize() const;

  // Getters:
  // ============ AP settings =============
  const std::string& getAdmissionPolicy() const { return admissionPolicy_; }
  double getAdmissionProbability() const { return admissionProbability_; }
  uint64_t getAdmissionWriteRate() const { return admissionWriteRate_; }
  uint64_t getMaxWriteRate() const { return maxWriteRate_; }
  size_t getAdmissionSuffixLength() const { return admissionSuffixLen_; }
  uint32_t getAdmissionProbBaseSize() const { return admissionProbBaseSize_; }

  // ============ Device settings =============
  uint64_t getBlockSize() const { return blockSize_; }
  const std::string& getFileName() const;
  const std::vector<std::string>& getRaidPaths() const;
  uint64_t getDeviceMetadataSize() const { return deviceMetadataSize_; }
  uint64_t getFileSize() const { return fileSize_; }
  bool getTruncateFile() const { return truncateFile_; }
  uint32_t getDeviceMaxWriteSize() const { return deviceMaxWriteSize_; }

  // ============ BlockCache settings =============
  bool getBlockCacheLru() const { return blockCacheLru_; }
  uint32_t getBlockCacheRegionSize() const { return blockCacheRegionSize_; }
  const std::vector<uint32_t>& getBlockCacheSizeClasses() const {
    return blockCacheSizeClasses_;
  }
  uint32_t getBlockCacheCleanRegions() const { return blockCacheCleanRegions_; }
  uint8_t getBlockCacheReinsertionHitsThreshold() const {
    return blockCacheReinsertionHitsThreshold_;
  }
  unsigned int getBlockCacheReinsertionProbabilityThreshold() const {
    return blockCacheReinsertionProbabilityThreshold_;
  }
  uint32_t getBlockCacheNumInMemBuffers() const {
    return blockCacheNumInMemBuffers_;
  }
  bool getBlockCacheDataChecksum() const { return blockCacheDataChecksum_; }
  const std::vector<unsigned int>& getBlockCacheSegmentedFifoSegmentRatio()
      const {
    return blockCacheSegmentedFifoSegmentRatio_;
  }

  // ============ BigHash settings =============
  unsigned int getBigHashSizePct() const { return bigHashConfig_.getSizePct(); }
  uint32_t getBigHashBucketSize() const {
    return bigHashConfig_.getBucketSize();
  }
  uint64_t getBigHashBucketBfSize() const {
    return bigHashConfig_.getBucketBfSize();
  }
  uint64_t getBigHashSmallItemMaxSize() const {
    return bigHashConfig_.getSmallItemMaxSize();
  }

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
  void setAdmissionPolicy(const std::string& admissionPolicy);
  // Set admission probability.
  // @throw std::std::invalid_argument if the admission policy is not
  //        "random" or the input value is not in the range of 0~1.
  void setAdmissionProbability(double admissionProbability);
  // Set admission policy target rate in bytes/s.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  void setAdmissionWriteRate(uint64_t admissionWriteRate);
  // Set the max write rate to device in bytes/s.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  void setMaxWriteRate(uint64_t maxWriteRate);
  // Set the length of suffix in key to be ignored when hashing for probability.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  void setAdmissionSuffixLength(size_t admissionSuffixLen);
  // Set the Navy item base size of baseProbability calculation.
  // @throw std::invalid_argument if the admission policy is not
  //        "dynamic_random".
  void setAdmissionProbBaseSize(uint32_t admissionProbBaseSize);

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
  void setBlockCacheLru(bool blockCacheLru);
  // Set segmentedFifoSegmentRatio for BlockCache.
  // @throw std::invalid_argument if LRU policy is used.
  void setBlockCacheSegmentedFifoSegmentRatio(
      std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio);
  // Set reinsertionHitsThreshold for BlockCache.
  // @throw std::invalid_argument if reinsertionProbabilityThreshold has been
  //        set.
  void setBlockCacheReinsertionHitsThreshold(
      uint8_t blockCacheReinsertionHitsThreshold);
  // Set ReinsertionProbabilityThreshold for BlockCache.
  // @throw std::invalid_argument if reinsertionHitsThreshold has been set or
  //        the input value is not in the range of 0~100.
  void setBlockCacheReinsertionProbabilityThreshold(
      unsigned int blockCacheReinsertionProbabilityThreshold);
  void setBlockCacheSizeClasses(
      std::vector<uint32_t> blockCacheSizeClasses) noexcept {
    blockCacheSizeClasses_ = std::move(blockCacheSizeClasses);
  }
  void setBlockCacheRegionSize(uint32_t blockCacheRegionSize) noexcept {
    blockCacheRegionSize_ = blockCacheRegionSize;
  }
  void setBlockCacheCleanRegions(uint32_t blockCacheCleanRegions) noexcept {
    blockCacheCleanRegions_ = blockCacheCleanRegions;
  }
  void setBlockCacheNumInMemBuffers(
      uint32_t blockCacheNumInMemBuffers) noexcept {
    blockCacheNumInMemBuffers_ = blockCacheNumInMemBuffers;
  }
  void setBlockCacheDataChecksum(bool blockCacheDataChecksum) noexcept {
    blockCacheDataChecksum_ = blockCacheDataChecksum;
  }

  // ============ BigHash settings =============
  // (Deprecated) Set the parameters for BigHash.
  // @throw std::invalid_argument if bigHashSizePct is not in the range of
  //        0~100.
  void setBigHash(unsigned int bigHashSizePct,
                  uint32_t bigHashBucketSize,
                  uint64_t bigHashBucketBfSize,
                  uint64_t bigHashSmallItemMaxSize);
  // Get BigHashConfig to configure bigHash.
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
  // Admission probability in decimal form.
  // This is used for "random" only.
  double admissionProbability_{};
  // Admission policy target rate, bytes/s.
  // Zero means no rate limiting.
  // This is used for "dynamic_random" only.
  uint64_t admissionWriteRate_{};
  // The max write rate to device in bytes/s to stay within the device limit of
  // saturation to avoid latency increase.
  // This is used for "dynamic_random" only.
  uint64_t maxWriteRate_{0};
  // Length of suffix in key to be ignored when hashing for probability.
  // This is used for "dynamic_random" only.
  size_t admissionSuffixLen_{0};
  // Navy item base size of baseProbability calculation.
  // This is used for "dynamic_random" only.
  size_t admissionProbBaseSize_{0};

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
  // Whether Navy BlockCache will use region-based LRU.
  bool blockCacheLru_{true};
  // Size for a region for Navy BlockCache (must be multiple of
  // blockSize_).
  uint32_t blockCacheRegionSize_{16 * 1024 * 1024};
  // A vector of Navy BlockCache size classes (must be multiples of
  // blockSize_).
  std::vector<uint32_t> blockCacheSizeClasses_;
  // Buffer of clean regions to maintain for eviction.
  uint32_t blockCacheCleanRegions_{1};
  // Threshold of a hits based reinsertion policy with Navy BlockCache.
  // If an item had been accessed more than that threshold, it will be eligible
  // for reinsertion.
  uint8_t blockCacheReinsertionHitsThreshold_{0};
  // Threshold of a probability based reinsertion policy with Navy BlockCache.
  // The probability value is between 0 and 100 for reinsertion.
  unsigned int blockCacheReinsertionProbabilityThreshold_{0};
  // Number of Navy BlockCache in-memory buffers.
  uint32_t blockCacheNumInMemBuffers_{};
  // Whether enabling data checksum for Navy BlockCache.
  bool blockCacheDataChecksum_{true};
  // An array of the ratio of each segment.
  // blockCacheLru_ must be false.
  std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio_;

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

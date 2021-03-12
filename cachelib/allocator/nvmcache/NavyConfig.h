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
 * It will be put as one part of NvmCache Config
 * https://fburl.com/diffusion/042f2o7x to deprecate the old folly::dynamic
 * way of doing the setup.
 *
 * Notes: the reason why these settings cannot be directly passed to Navy
 * internal config https://fburl.com/diffusion/y5fqozuy and setup there is
 * because we have logic in "NavySetup.cpp" that translates this input config
 * into CacheProto. Therefore, we need this intermediary config in NvmCache
 * Config.
 *
 */
class NavyConfig {
 public:
  bool usesSimpleFile() const { return !fileName_.empty(); }
  bool usesRaidFiles() const { return raidPaths_.size() > 0; }
  std::map<std::string, std::string> serialize() const;

  // Getters:
  // admission policy settings
  const std::string& getAdmissionPolicy() const { return admissionPolicy_; }
  double getAdmissionsProbability() const { return admissionProbability_; }
  uint64_t getAdmissionWriteRate() const { return admissionWriteRate_; }
  uint64_t getMaxWriteRate() const { return maxWriteRate_; }
  size_t getAdmissionSuffixLength() const { return admissionSuffixLen_; }
  uint32_t getAdmissionProbBaseSize() const { return admissionProbBaseSize_; }

  // device settings
  uint64_t getBlockSize() const { return blockSize_; }
  const std::string& getFileName() const;
  const std::vector<std::string>& getRaidPaths() const;
  uint64_t getDeviceMetadataSize() const { return deviceMetadataSize_; }
  uint64_t getFileSize() const { return fileSize_; }
  bool getTruncateFile() const { return truncateFile_; }
  uint32_t getDeviceMaxWriteSize() const { return deviceMaxWriteSize_; }

  // BlockCache settings
  bool getBlockCacheLru() const { return blockCacheLru_; }
  uint64_t getBlockCacheRegionSize() const { return blockCacheRegionSize_; }
  uint64_t getBlockCacheReadBufferSize() const {
    return blockCacheReadBufferSize_;
  }
  const std::vector<uint32_t>& getBlockCacheSizeClasses() const {
    return blockCacheSizeClasses_;
  }
  uint32_t getBlockCacheCleanRegions() const { return blockCacheCleanRegions_; }
  uint64_t getBlockCacheReinsertionHitsThreshold() const {
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

  // BigCache settings
  unsigned int getBigHashSizePct() const { return bigHashSizePct_; }
  uint64_t getBigHashBucketSize() const { return bigHashBucketSize_; }
  uint64_t getBigHashBucketBfSize() const { return bigHashBucketBfSize_; }
  uint64_t getBigHashSmallItemMaxSize() const {
    return bigHashSmallItemMaxSize_;
  }

  // job scheduler settings
  unsigned int getReaderThreads() const { return readerThreads_; }
  unsigned int getWriterThreads() const { return writerThreads_; }
  uint64_t getNavyReqOrderingShards() const { return navyReqOrderingShards_; }

  // other settings
  uint32_t getMaxConcurrentInserts() const { return maxConcurrentInserts_; }
  uint64_t getMaxParcelMemoryMB() const { return maxParcelMemoryMB_; }

  // Setters:
  // admission policy settings
  void setAdmissionPolicy(const std::string& admissionPolicy) {
    admissionPolicy_ = admissionPolicy;
  }
  void setAdmissionsProbability(double admissionProbability) {
    admissionProbability_ = admissionProbability;
  }
  void setAdmissionWriteRate(uint64_t admissionWriteRate) {
    admissionWriteRate_ = admissionWriteRate;
  }
  void setMaxWriteRate(uint64_t maxWriteRate) { maxWriteRate_ = maxWriteRate; }

  void setAdmissionSuffixLength(size_t admissionSuffixLen) {
    admissionSuffixLen_ = admissionSuffixLen;
  }
  void setAdmissionProbBaseSize(uint32_t admissionProbBaseSize) {
    admissionProbBaseSize_ = admissionProbBaseSize;
  }

  // device settings
  void setBlockSize(uint64_t blockSize) { blockSize_ = blockSize; }
  void setFileName(const std::string& fileName) { fileName_ = fileName; }
  void setRaidPaths(const std::vector<std::string>& raidPaths) {
    raidPaths_ = raidPaths;
  }
  void setDeviceMetadataSize(uint64_t deviceMetadataSize) {
    deviceMetadataSize_ = deviceMetadataSize;
  }
  void setFileSize(uint64_t fileSize) { fileSize_ = fileSize; }
  void setTruncateFile(bool truncateFile) { truncateFile_ = truncateFile; }
  void setDeviceMaxWriteSize(uint32_t deviceMaxWriteSize) {
    deviceMaxWriteSize_ = deviceMaxWriteSize;
  }

  // BlockCache settings
  void setBlockCacheLru(bool blockCacheLru) { blockCacheLru_ = blockCacheLru; }
  void setBlockCacheRegionSize(uint64_t blockCacheRegionSize) {
    blockCacheRegionSize_ = blockCacheRegionSize;
  }
  void setBlockCacheReadBufferSize(uint64_t blockCacheReadBufferSize) {
    blockCacheReadBufferSize_ = blockCacheReadBufferSize;
  }
  void setBlockCacheSizeClasses(
      const std::vector<uint32_t>& blockCacheSizeClasses) {
    blockCacheSizeClasses_ = blockCacheSizeClasses;
  }
  void setBlockCacheCleanRegions(uint32_t blockCacheCleanRegions) {
    blockCacheCleanRegions_ = blockCacheCleanRegions;
  }
  void setBlockCacheReinsertionHitsThreshold(
      uint64_t blockCacheReinsertionHitsThreshold) {
    blockCacheReinsertionHitsThreshold_ = blockCacheReinsertionHitsThreshold;
  }
  void setBlockCacheReinsertionProbabilityThreshold(
      unsigned int blockCacheReinsertionProbabilityThreshold) {
    blockCacheReinsertionProbabilityThreshold_ =
        blockCacheReinsertionProbabilityThreshold;
  }
  void setBlockCacheNumInMemBuffers(uint32_t blockCacheNumInMemBuffers) {
    blockCacheNumInMemBuffers_ = blockCacheNumInMemBuffers;
  }
  void setBlockCacheDataChecksum(bool blockCacheDataChecksum) {
    blockCacheDataChecksum_ = blockCacheDataChecksum;
  }
  void setBlockCacheSegmentedFifoSegmentRatio(
      const std::vector<unsigned int>& blockCacheSegmentedFifoSegmentRatio) {
    blockCacheSegmentedFifoSegmentRatio_ = blockCacheSegmentedFifoSegmentRatio;
  }

  // BigHash settings
  void setBigHashSizePct(unsigned int bigHashSizePct) {
    bigHashSizePct_ = bigHashSizePct;
  }
  void setBigHashBucketSize(uint64_t bigHashBucketSize) {
    bigHashBucketSize_ = bigHashBucketSize;
  }
  void setBigHashBucketBfSize(uint64_t bigHashBucketBfSize) {
    bigHashBucketBfSize_ = bigHashBucketBfSize;
  }
  void setBigHashSmallItemMaxSize(uint64_t bigHashSmallItemMaxSize) {
    bigHashSmallItemMaxSize_ = bigHashSmallItemMaxSize;
  }

  // job scheduler settings
  void setReaderThreads(unsigned int readerThreads) {
    readerThreads_ = readerThreads;
  }
  void setWriterThreads(unsigned int writerThreads) {
    writerThreads_ = writerThreads;
  }
  void setNavyReqOrderingShards(uint64_t navyReqOrderingShards);

  // other settings
  void setMaxConcurrentInserts(uint32_t maxConcurrentInserts) {
    maxConcurrentInserts_ = maxConcurrentInserts;
  }
  void setMaxParcelMemoryMB(uint64_t maxParcelMemoryMB) {
    maxParcelMemoryMB_ = maxParcelMemoryMB;
  }

 private:
  // ============ AP settings =============
  // Name of the admission policy.
  // This could only be "dynamic_random" or "random" (or empty).
  std::string admissionPolicy_;
  // Admission probability in decimal form.
  // This is used for "random" only.
  double admissionProbability_;
  // Admission policy target rate, bytes/s.
  // Zero means no rate limiting.
  // This is used for "dynamic_random" only.
  uint64_t admissionWriteRate_;
  // The max write rate to device in bytes/s to stay within the device limit of
  // saturation to avoid latency increase.
  // This is used for "dynamic_random" only.
  uint64_t maxWriteRate_{0};
  // Length of suffix in key to be ignored when hashing for probability.
  // This is used for "dynamic_random" only.
  size_t admissionSuffixLen_{0};
  // Navy item base size of baseProbability calculation.
  // This is used for "dynamic_random" only.
  size_t admissionProbBaseSize_;

  // ============ Device settings =============
  // Navy specific device block size in bytes.
  uint64_t blockSize_{4096};
  // The file name/path for caching.
  std::string fileName_;
  // An array of Navy RAID device file paths.
  std::vector<std::string> raidPaths_;
  // The size of the metadata partition on the Navy device.
  uint64_t deviceMetadataSize_;
  // The size of the file that Navy should use.
  // 0 means to use the whole device.
  uint64_t fileSize_;
  // Whether ask Navy to truncate the file it uses.
  bool truncateFile_{false};
  // This controls granularity of the writes when we flush the region.
  // This is only used when in-mem buffer is enabled.
  uint32_t deviceMaxWriteSize_;

  // ============ BlockCache settings =============
  // Whether Navy BlockCache will use region-based LRU.
  bool blockCacheLru_{true};
  // Size for a region for Navy BlockCache (must be multiple of
  // blockSize_).
  uint64_t blockCacheRegionSize_{16 * 1024 * 1024};
  // Read buffer size for stack allocator.
  // In stacked mode, we will read fixed buffer on first attempt. If we read
  // too little, we will read again. This size should be sized to ensure we
  // can do most of the reads with just a single IO.
  uint64_t blockCacheReadBufferSize_{4096};
  // A vector of Navy BlockCache size classes (must be multiples of
  // blockSize_).
  std::vector<uint32_t> blockCacheSizeClasses_;
  // Buffer of clean regions to maintain for eviction.
  uint32_t blockCacheCleanRegions_{1};
  // Threshold of a hits based reinsertion policy with Navy BlockCache.
  // If an item had been accessed more than that threshold, it will be eligible
  // for reinsertion.
  uint64_t blockCacheReinsertionHitsThreshold_{0};
  // Threshold of a probability based reinsertion policy with Navy BlockCache.
  // The probability value is between 0 and 100 for reinsertion.
  unsigned int blockCacheReinsertionProbabilityThreshold_{0};
  // Number of Navy BlockCache in-memory buffers.
  uint32_t blockCacheNumInMemBuffers_;
  // Whether enabling data checksum for Navy BlockCache.
  bool blockCacheDataChecksum_{true};
  // An array of the ratio of each segment.
  // blockCacheLru_ must be false.
  std::vector<unsigned int> blockCacheSegmentedFifoSegmentRatio_;

  // ============ BigHash settings =============
  // Percentage of how much of the device out of all is given to BigHash
  // engine in Navy, e.g. 50.
  unsigned int bigHashSizePct_{0};
  // Navy BigHash engine's bucket size (must be multiple of the minimum
  // device io block size).
  // This size determines how big each bucket is and what is the physical
  // write granularity onto the device.
  uint64_t bigHashBucketSize_{4096};
  // The bloom filter size per bucket in bytes for Navy BigHash engine
  uint64_t bigHashBucketBfSize_{8};
  // The maximum item size to put into Navy BigHash engine.
  uint64_t bigHashSmallItemMaxSize_;

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

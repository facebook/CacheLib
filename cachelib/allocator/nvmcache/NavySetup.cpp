#include "cachelib/allocator/nvmcache/NavySetup.h"

#include <folly/File.h>
#include <folly/logging/xlog.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/Factory.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {

namespace {

// Default value for (almost) 1TB flash device = 5GB reserved for metadata
constexpr double kDefaultMetadataPercent = 0.5;
uint64_t megabytesToBytes(uint64_t mb) { return mb << 20; }

// Return a number that's equal or smaller than @num and aligned on @alignment
uint64_t alignDown(uint64_t num, uint64_t alignment) {
  return num - num % alignment;
}

// Return a number that's equal or bigger than @num and aligned on @alignment
uint64_t alignUp(uint64_t num, uint64_t alignment) {
  return alignDown(num + alignment - 1, alignment);
}

// Open cache file @fileName and set it size to @size.
// Throws std::system_error if failed.
folly::File openCacheFile(const std::string& fileName,
                          uint64_t size,
                          bool truncate) {
  XLOG(INFO) << "Cache file: " << fileName << " size: " << size
             << " truncate: " << truncate;
  if (fileName.empty()) {
    throw std::invalid_argument("File name is empty");
  }

  int flags{O_RDWR | O_CREAT};
  // try opening with o_direct. For tests, we might get a file on tmpfs that
  // might not support o_direct. Hence, we might have to default to avoiding
  // o_direct in those cases.
  folly::File f;
  try {
    f = folly::File(fileName.c_str(), flags | O_DIRECT);
  } catch (const std::system_error& e) {
    if (e.code().value() == EINVAL) {
      XLOG(ERR) << "Failed to open with o-direct, trying without. Error: "
                << e.what();
      f = folly::File(fileName.c_str(), flags);
    }
  }
  XDCHECK_GE(f.fd(), 0);

  // TODO detect if file exists and is of expected size. If not,
  // automatically fallocate the file or ftruncate the file.
  if (truncate && ::fallocate(f.fd(), 0, 0, size) < 0) {
    throw std::system_error(
        errno,
        std::system_category(),
        folly::sformat("failed fallocate with size {}", size));
  }

  if (::posix_fadvise(f.fd(), 0, size, POSIX_FADV_DONTNEED) < 0) {
    throw std::system_error(errno, std::system_category(),
                            "Error fadvising cache file");
  }

  return f;
}

void setupCacheProtosImpl(const navy::Device& device,
                          uint64_t metadataSize,
                          const unsigned int bigHashPctSize,
                          const uint32_t bucketSize,
                          const uint64_t bfSize,
                          const uint64_t smallItemMaxSize,
                          const uint32_t regionSize,
                          const bool dataChecksum,
                          std::vector<unsigned int> segmentRatio,
                          const bool useLru,
                          std::vector<uint32_t> sizeClasses,
                          const uint32_t cleanRegions,
                          const uint8_t reinsertionHitsThreshold,
                          const uint32_t reinsertionProbabilityThreshold,
                          const uint32_t numInMemBuffers,
                          const bool usesRaidFiles,
                          cachelib::navy::CacheProto& proto) {
  const uint64_t totalCacheSize = device.getSize();

  auto ioAlignSize = device.getIOAlignmentSize();
  auto getDefaultMetadataSize = [](size_t size, size_t alignment) {
    XDCHECK(folly::isPowTwo(alignment));
    auto mask = ~(alignment - 1);
    return (static_cast<size_t>(kDefaultMetadataPercent * size / 100) & mask);
  };
  if (metadataSize == 0) {
    metadataSize = getDefaultMetadataSize(totalCacheSize, ioAlignSize);
  }

  metadataSize = alignUp(metadataSize, ioAlignSize);

  if (metadataSize >= totalCacheSize) {
    throw std::invalid_argument{
        folly::sformat("Invalid metadata size: {}. Cache size: {}",
                       metadataSize,
                       totalCacheSize)};
  }

  proto.setMetadataSize(metadataSize);
  uint64_t blockCacheSize = 0;

  // Set up BigHash if enabled
  if (bigHashPctSize > 0) {
    if (bucketSize != alignUp(bucketSize, ioAlignSize)) {
      throw std::invalid_argument(
          folly::sformat("Bucket size: {} is not aligned to ioAlignSize: {}",
                         bucketSize, ioAlignSize));
    }

    // If enabled, BigHash's storage starts after BlockCache's.
    const auto sizeReservedForBigHash = totalCacheSize * bigHashPctSize / 100ul;

    const uint64_t bigHashCacheOffset =
        alignUp(totalCacheSize - sizeReservedForBigHash, bucketSize);
    const uint64_t bigHashCacheSize =
        alignDown(totalCacheSize - bigHashCacheOffset, bucketSize);

    auto bigHash = cachelib::navy::createBigHashProto();
    bigHash->setLayout(bigHashCacheOffset, bigHashCacheSize, bucketSize);

    // Bucket Bloom filter size, bytes
    //
    // Experiments showed that if we have 16 bytes for BF with 25 entries,
    // then optimal number of hash functions is 4 and false positive rate
    // below 10%. See details:
    // https://fb.facebook.com/groups/522950611436641/permalink/579237922474576/
    if (bfSize > 0) {
      // We set 4 hash function unconditionally. This seems to be the best
      // for our use case. If BF size to bucket size ratio gets lower, try
      // to reduce number of hashes.
      constexpr uint32_t kNumHashes = 4;
      const uint32_t bitsPerHash = bfSize * 8 / kNumHashes;
      bigHash->setBloomFilter(kNumHashes, bitsPerHash);
    }

    proto.setBigHash(std::move(bigHash), smallItemMaxSize);

    if (bigHashCacheOffset <= metadataSize) {
      throw std::invalid_argument("NVM cache size is not big enough!");
    }
    blockCacheSize = bigHashCacheOffset - metadataSize;
    XLOG(INFO) << "metadataSize: " << metadataSize
               << " bigHashCacheOffset: " << bigHashCacheOffset
               << " bigHashCacheSize: " << bigHashCacheSize;
  } else {
    blockCacheSize = totalCacheSize - metadataSize;
    XLOG(INFO) << "metadataSize: " << metadataSize << ". No bighash.";
  }

  // Set up BlockCache if enabled
  if (blockCacheSize > 0) {
    if (regionSize != alignUp(regionSize, ioAlignSize)) {
      throw std::invalid_argument(
          folly::sformat("Region size: {} is not aligned to ioAlignSize: {}",
                         regionSize, ioAlignSize));
    }

    // Adjust starting size of block cache to ensure it is aligned to region
    // size which is what we use for the stripe size when using RAID0Device.
    uint64_t blockCacheOffset = metadataSize;
    if (usesRaidFiles) {
      auto adjustedBlockCacheOffset = alignUp(blockCacheOffset, regionSize);
      auto cacheSizeAdjustment = adjustedBlockCacheOffset - blockCacheOffset;
      XDCHECK_LT(cacheSizeAdjustment, blockCacheSize);
      blockCacheSize -= cacheSizeAdjustment;
      blockCacheOffset = adjustedBlockCacheOffset;
    }
    blockCacheSize = alignDown(blockCacheSize, regionSize);

    XLOG(INFO) << "blockcache: starting offset: " << blockCacheOffset
               << ", block cache size: " << blockCacheSize;

    auto blockCache = cachelib::navy::createBlockCacheProto();
    blockCache->setLayout(blockCacheOffset, blockCacheSize, regionSize);
    blockCache->setChecksum(dataChecksum);

    if (!segmentRatio.empty()) {
      blockCache->setSegmentedFifoEvictionPolicy(std::move(segmentRatio));
    } else if (useLru) {
      blockCache->setLruEvictionPolicy();
    } else {
      blockCache->setFifoEvictionPolicy();
    }

    if (!sizeClasses.empty()) {
      blockCache->setSizeClasses(std::move(sizeClasses));
    }
    blockCache->setCleanRegionsPool(cleanRegions);

    if (reinsertionHitsThreshold > 0) {
      blockCache->setHitsReinsertionPolicy(reinsertionHitsThreshold);
    }

    if (reinsertionProbabilityThreshold > 0) {
      blockCache->setProbabilisticReinsertionPolicy(
          reinsertionProbabilityThreshold);
    }

    blockCache->setNumInMemBuffers(numInMemBuffers);

    proto.setBlockCache(std::move(blockCache));
  }
}

void setupCacheProtos(const navy::NavyConfig& config,
                      const navy::Device& device,
                      cachelib::navy::CacheProto& proto) {
  auto metadataSize = config.getDeviceMetadataSize();
  setupCacheProtosImpl(device,
                       metadataSize,
                       config.getBigHashSizePct(),
                       config.getBigHashBucketSize(),
                       config.getBigHashBucketBfSize(),
                       config.getBigHashSmallItemMaxSize(),
                       config.getBlockCacheRegionSize(),
                       config.getBlockCacheDataChecksum(),
                       config.getBlockCacheSegmentedFifoSegmentRatio(),
                       config.getBlockCacheLru(),
                       config.getBlockCacheSizeClasses(),
                       config.getBlockCacheCleanRegions(),
                       config.getBlockCacheReinsertionHitsThreshold(),
                       config.getBlockCacheReinsertionProbabilityThreshold(),
                       config.getBlockCacheNumInMemBuffers(),
                       config.usesRaidFiles(),
                       proto);
}

void setAdmissionPolicy(const cachelib::navy::NavyConfig& config,
                        cachelib::navy::CacheProto& proto) {
  const std::string& policyName = config.getAdmissionPolicy();
  if (policyName.empty()) {
    return;
  }
  if (policyName == navy::NavyConfig::kAdmPolicyRandom) {
    proto.setRejectRandomAdmissionPolicy(config.getAdmissionProbability());
  } else if (policyName == navy::NavyConfig::kAdmPolicyDynamicRandom) {
    proto.setDynamicRandomAdmissionPolicy(
        config.getAdmissionWriteRate(), config.getAdmissionSuffixLength(),
        config.getAdmissionProbBaseSize(), config.getMaxWriteRate());
  } else {
    throw std::invalid_argument{
        folly::sformat("invalid policy name {}", policyName)};
  }
}

std::unique_ptr<cachelib::navy::JobScheduler> createJobScheduler(
    const navy::NavyConfig& config) {
  auto readerThreads = config.getReaderThreads();
  auto writerThreads = config.getWriterThreads();
  auto reqOrderShardsPower = config.getNavyReqOrderingShards();
  return cachelib::navy::createOrderedThreadPoolJobScheduler(
      readerThreads, writerThreads, reqOrderShardsPower);
}
} // namespace

std::unique_ptr<cachelib::navy::Device> createDevice(
    const navy::NavyConfig& config,
    std::shared_ptr<navy::DeviceEncryptor> encryptor) {
  auto blockSize = config.getBlockSize();
  auto maxDeviceWriteSize = config.getDeviceMaxWriteSize();
  if (maxDeviceWriteSize > 0) {
    maxDeviceWriteSize = alignDown(maxDeviceWriteSize, blockSize);
  };

  if (config.usesRaidFiles()) {
    auto raidPaths = config.getRaidPaths();

    // File paths are opened in the increasing order of the
    // path string. This ensures that RAID0 stripes aren't
    // out of order even if the caller changes the order of
    // the file paths. We can recover the cache as long as all
    // the paths are specified, regardless of the order.

    std::sort(raidPaths.begin(), raidPaths.end());

    auto fdSize = config.getFileSize();
    std::vector<folly::File> fileVec;
    for (const auto& path : raidPaths) {
      folly::File f;
      try {
        f = openCacheFile(path, fdSize, config.getTruncateFile());
      } catch (const std::exception& e) {
        XLOG(ERR) << "Exception in openCacheFile: " << path << e.what()
                  << ". Errno: " << errno;
        throw;
      }
      fileVec.push_back(std::move(f));
    }

    // Align down device size to ensure each device is aligned to stripe size
    auto stripeSize = config.getBlockCacheRegionSize();
    fdSize = alignDown(fdSize, stripeSize);

    auto device =
        cachelib::navy::createDirectIoRAID0Device(std::move(fileVec),
                                                  fdSize,
                                                  blockSize,
                                                  stripeSize,
                                                  std::move(encryptor),
                                                  maxDeviceWriteSize);
    return device;
  }

  const auto singleFileSize = config.getFileSize();
  if (config.usesSimpleFile()) {
    // Create a simple file device
    folly::File f;
    try {
      f = openCacheFile(config.getFileName(), singleFileSize,
                        config.getTruncateFile());
    } catch (const std::exception& e) {
      XLOG(ERR) << "Exception in openCacheFile: " << e.what();
      throw;
    }
    return cachelib::navy::createDirectIoFileDevice(
        std::move(f), singleFileSize, blockSize, std::move(encryptor),
        maxDeviceWriteSize);
  }
  return cachelib::navy::createMemoryDevice(singleFileSize,
                                            std::move(encryptor), blockSize);
}

std::unique_ptr<navy::AbstractCache> createNavyCache(
    const navy::NavyConfig& config,
    navy::DestructorCallback cb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor) {
  auto device = createDevice(config, std::move(encryptor));

  auto proto = cachelib::navy::createCacheProto();
  auto* devicePtr = device.get();
  proto->setDevice(std::move(device));
  proto->setJobScheduler(createJobScheduler(config));
  proto->setMaxConcurrentInserts(config.getMaxConcurrentInserts());
  proto->setMaxParcelMemory(megabytesToBytes(config.getMaxParcelMemoryMB()));
  setAdmissionPolicy(config, *proto);
  proto->setDestructorCallback(cb);

  setupCacheProtos(config, *devicePtr, *proto);

  auto cache = createCache(std::move(proto));
  XDCHECK(cache != nullptr);

  if (truncate) {
    cache->reset();
    return cache;
  }

  if (!cache->recover()) {
    XLOG(WARN) << "No recovery data found. Continuing with clean cache.";
  }
  return cache;
}
} // namespace cachelib
} // namespace facebook

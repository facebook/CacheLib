
#include "cachelib/navy/Factory.h"

#include <folly/Format.h>
#include <folly/Random.h>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/admission_policy/RejectRandomAP.h"
#include "cachelib/navy/bighash/BigHash.h"
#include "cachelib/navy/block_cache/BlockCache.h"
#include "cachelib/navy/block_cache/FifoPolicy.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/LruPolicy.h"
#include "cachelib/navy/block_cache/PercentageReinsertionPolicy.h"
#include "cachelib/navy/driver/Driver.h"
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
class BlockCacheProtoImpl final : public BlockCacheProto {
 public:
  BlockCacheProtoImpl() = default;
  ~BlockCacheProtoImpl() override = default;

  void setLayout(uint64_t baseOffset,
                 uint64_t size,
                 uint32_t regionSize) override {
    if (size <= 0 || regionSize <= 0) {
      throw std::invalid_argument(folly::sformat(
          "Invalid layout. size: {}, regionSize: {}.", size, regionSize));
    }
    config_.cacheBaseOffset = baseOffset;
    config_.cacheSize = size;
    config_.regionSize = regionSize;
  }

  void setChecksum(bool enable) override { config_.checksum = enable; }

  void setLruEvictionPolicy() override {
    if (!(config_.cacheSize > 0 && config_.regionSize > 0)) {
      throw std::logic_error("layout is not set");
    }
    auto numRegions = config_.getNumRegions();
    if (config_.evictionPolicy) {
      throw std::invalid_argument("There's already an eviction policy set");
    }
    config_.evictionPolicy = std::make_unique<LruPolicy>(numRegions);
  }

  void setFifoEvictionPolicy() override {
    if (config_.evictionPolicy) {
      throw std::invalid_argument("There's already an eviction policy set");
    }
    config_.evictionPolicy = std::make_unique<FifoPolicy>();
  }

  void setSegmentedFifoEvictionPolicy(
      std::vector<unsigned int> segmentRatio) override {
    if (config_.evictionPolicy) {
      throw std::invalid_argument("There's already an eviction policy set");
    }
    config_.numPriorities = static_cast<uint16_t>(segmentRatio.size());
    config_.evictionPolicy =
        std::make_unique<SegmentedFifoPolicy>(std::move(segmentRatio));
  }

  void setSizeClasses(std::vector<uint32_t> sizeClasses) override {
    config_.sizeClasses = std::move(sizeClasses);
  }

  void setReadBufferSize(uint32_t size) override {
    config_.readBufferSize = size;
  }

  void setCleanRegionsPool(uint32_t n) override {
    config_.cleanRegionsPool = n;
  }

  void setHitsReinsertionPolicy(uint8_t reinsertionThreshold) override {
    if (config_.reinsertionPolicy) {
      throw std::invalid_argument("There's already a reinsertion policy set.");
    }
    config_.reinsertionPolicy =
        std::make_unique<HitsReinsertionPolicy>(reinsertionThreshold);
  }

  void setPercentageReinsertionPolicy(uint32_t percentage) override {
    if (config_.reinsertionPolicy) {
      throw std::invalid_argument("There's already a reinsertion policy set.");
    }
    config_.reinsertionPolicy =
        std::make_unique<PercentageReinsertionPolicy>(percentage);
  }

  void setDevice(Device* device) { config_.device = device; }

  void setNumInMemBuffers(uint32_t numInMemBuffers) override {
    config_.numInMemBuffers = numInMemBuffers;
  }

  std::unique_ptr<Engine> create(JobScheduler& scheduler,
                                 DestructorCallback cb) && {
    config_.scheduler = &scheduler;
    config_.destructorCb = std::move(cb);
    config_.validate();
    return std::make_unique<BlockCache>(std::move(config_));
  }

 private:
  BlockCache::Config config_;
};

class BigHashProtoImpl final : public BigHashProto {
 public:
  BigHashProtoImpl() = default;
  ~BigHashProtoImpl() override = default;

  void setLayout(uint64_t baseOffset,
                 uint64_t size,
                 uint32_t bucketSize) override {
    config_.cacheBaseOffset = baseOffset;
    config_.cacheSize = size;
    config_.bucketSize = bucketSize;
  }

  // BigHash uses bloom filters (BF) to reduce number of IO. We want to maintain
  // BF for every bucket, because we want to rebuild it on every remove to keep
  // its filtering properties.
  void setBloomFilter(uint32_t numHashes, uint32_t hashTableBitSize) override {
    // Want to make @setLayout and Bloom filter setup independent.
    bloomFilterEnabled_ = true;
    numHashes_ = numHashes;
    hashTableBitSize_ = hashTableBitSize;
  }

  void setDevice(Device* device) { config_.device = device; }

  void setDestructorCb(DestructorCallback cb) {
    config_.destructorCb = std::move(cb);
  }

  std::unique_ptr<Engine> create() && {
    if (bloomFilterEnabled_) {
      if (config_.bucketSize == 0) {
        throw std::invalid_argument{"invalid bucket size"};
      }
      config_.bloomFilter = std::make_unique<BloomFilter>(
          config_.numBuckets(), numHashes_, hashTableBitSize_);
    }
    return std::make_unique<BigHash>(std::move(config_));
  }

 private:
  BigHash::Config config_;
  bool bloomFilterEnabled_{false};
  uint32_t numHashes_{};
  uint32_t hashTableBitSize_{};
};

class CacheProtoImpl final : public CacheProto {
 public:
  CacheProtoImpl() = default;
  ~CacheProtoImpl() override = default;

  void setMaxConcurrentInserts(uint32_t limit) override {
    config_.maxConcurrentInserts = limit;
  }

  void setMaxParcelMemory(uint64_t limit) override {
    config_.maxParcelMemory = limit;
  }

  void setDevice(std::unique_ptr<Device> device) override {
    config_.device = std::move(device);
  }

  void setMetadataSize(size_t size) override { config_.metadataSize = size; }

  void setBlockCache(std::unique_ptr<BlockCacheProto> proto) override {
    blockCacheProto_ = std::move(proto);
  }

  void setBigHash(std::unique_ptr<BigHashProto> proto,
                  uint32_t smallItemMaxSize) override {
    bigHashProto_ = std::move(proto);
    config_.smallItemMaxSize = smallItemMaxSize;
  }

  void setDestructorCallback(DestructorCallback cb) override {
    destructorCb_ = std::move(cb);
  }

  void setRejectRandomAdmissionPolicy(double probability) override {
    RejectRandomAP::Config apConfig;
    apConfig.probability = probability;
    apConfig.seed = folly::Random::rand32();
    config_.admissionPolicy =
        std::make_unique<RejectRandomAP>(std::move(apConfig));
  }

  void setDynamicRandomAdmissionPolicy(uint64_t targetRate,
                                       size_t deterministicKeyHashSuffixLength,
                                       uint32_t itemBaseSize,
                                       uint64_t maxRate) override {
    DynamicRandomAP::Config apConfig;
    apConfig.targetRate = targetRate;
    apConfig.fnBytesWritten = [device = config_.device.get()]() {
      return device->getBytesWritten();
    };
    apConfig.seed = folly::Random::rand32();
    apConfig.deterministicKeyHashSuffixLength =
        deterministicKeyHashSuffixLength;
    if (itemBaseSize > 0) {
      apConfig.baseSize = itemBaseSize;
    }
    if (maxRate > 0) {
      apConfig.maxRate = maxRate;
    }
    config_.admissionPolicy =
        std::make_unique<DynamicRandomAP>(std::move(apConfig));
  }

  void setJobScheduler(std::unique_ptr<JobScheduler> ex) override {
    config_.scheduler = std::move(ex);
  }

  std::unique_ptr<AbstractCache> create() && {
    if (config_.scheduler == nullptr) {
      throw std::invalid_argument("scheduler is not set");
    }

    if (blockCacheProto_) {
      auto bcProto = dynamic_cast<BlockCacheProtoImpl*>(blockCacheProto_.get());
      if (bcProto != nullptr) {
        bcProto->setDevice(config_.device.get());
        config_.largeItemCache =
            std::move(*bcProto).create(*config_.scheduler, destructorCb_);
      }
    }

    if (bigHashProto_) {
      auto bhProto = dynamic_cast<BigHashProtoImpl*>(bigHashProto_.get());
      if (bhProto != nullptr) {
        bhProto->setDevice(config_.device.get());
        bhProto->setDestructorCb(destructorCb_);
        config_.smallItemCache = std::move(*bhProto).create();
      }
    }

    return std::make_unique<Driver>(std::move(config_));
  }

 private:
  DestructorCallback destructorCb_;
  std::unique_ptr<BlockCacheProto> blockCacheProto_;
  std::unique_ptr<BigHashProto> bigHashProto_;
  Driver::Config config_;
};
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
} // namespace

std::unique_ptr<BlockCacheProto> createBlockCacheProto() {
  return std::make_unique<BlockCacheProtoImpl>();
}

std::unique_ptr<BigHashProto> createBigHashProto() {
  return std::make_unique<BigHashProtoImpl>();
}

std::unique_ptr<CacheProto> createCacheProto() {
  return std::make_unique<CacheProtoImpl>();
}

std::unique_ptr<AbstractCache> createCache(std::unique_ptr<CacheProto> proto) {
  return std::move(dynamic_cast<CacheProtoImpl&>(*proto)).create();
}

std::unique_ptr<Device> createRAIDDevice(
    std::vector<std::string> raidPaths,
    uint64_t fdSize,
    bool truncateFile,
    uint32_t blockSize,
    uint32_t stripeSize,
    std::shared_ptr<navy::DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize) {
  // File paths are opened in the increasing order of the
  // path string. This ensures that RAID0 stripes aren't
  // out of order even if the caller changes the order of
  // the file paths. We can recover the cache as long as all
  // the paths are specified, regardless of the order.

  std::sort(raidPaths.begin(), raidPaths.end());
  std::vector<folly::File> fileVec;
  for (const auto& path : raidPaths) {
    folly::File f;
    try {
      f = openCacheFile(path, fdSize, truncateFile);
    } catch (const std::exception& e) {
      XLOG(ERR) << "Exception in openCacheFile: " << path << e.what()
                << ". Errno: " << errno;
      throw;
    }
    fileVec.push_back(std::move(f));
  }

  return createDirectIoRAID0Device(std::move(fileVec),
                                   fdSize,
                                   blockSize,
                                   stripeSize,
                                   std::move(encryptor),
                                   maxDeviceWriteSize);
}

std::unique_ptr<Device> createFileDevice(
    std::string fileName,
    uint64_t singleFileSize,
    bool truncateFile,
    uint32_t blockSize,
    std::shared_ptr<navy::DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize) {
  folly::File f;
  try {
    f = openCacheFile(fileName, singleFileSize, truncateFile);
  } catch (const std::exception& e) {
    XLOG(ERR) << "Exception in openCacheFile: " << e.what();
    throw;
  }
  return createDirectIoFileDevice(std::move(f), singleFileSize, blockSize,
                                  std::move(encryptor), maxDeviceWriteSize);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

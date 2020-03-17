#include "cachelib/allocator/nvmcache/NavySetup.h"
#include "cachelib/navy/Factory.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"

#include <folly/File.h>
#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {

namespace {

// Default value for (almost) 1TB flash device = 5GB reserved for metadata
constexpr double kDefaultMetadataPercent = 0.5;

constexpr folly::StringPiece kAdmissionPolicy{"dipper_navy_adm_policy"};
constexpr folly::StringPiece kAdmissionProb{"dipper_navy_adm_probability"};
constexpr folly::StringPiece kAdmissionWriteRate{"dipper_navy_adm_write_rate"};
constexpr folly::StringPiece kAdmissionSuffixLen{
    "dipper_navy_adm_suffix_length"};
constexpr folly::StringPiece kBlockSize{"dipper_navy_block_size"};
constexpr folly::StringPiece kDirectIO{"dipper_navy_direct_io"};
constexpr folly::StringPiece kFileName{"dipper_navy_file_name"};
constexpr folly::StringPiece kRAIDPaths{"dipper_navy_raid_paths"};
constexpr folly::StringPiece kDeviceMetadataSize{"dipper_navy_metadata_size"};
constexpr folly::StringPiece kFileSize{"dipper_navy_file_size"};
constexpr folly::StringPiece kTruncateFile{"dipper_navy_truncate_file"};
constexpr folly::StringPiece kLru{"dipper_navy_lru"};
constexpr folly::StringPiece kRegionSize{"dipper_navy_region_size"};
constexpr folly::StringPiece kReadBuffer{"dipper_navy_read_buffer"};
constexpr folly::StringPiece kSizeClasses{"dipper_navy_size_classes"};
constexpr folly::StringPiece kBigHashSizePct{"dipper_navy_bighash_size_pct"};
constexpr folly::StringPiece kBigHashBucketSize{
    "dipper_navy_bighash_bucket_size"};
constexpr folly::StringPiece kBigHashBucketBFSize{
    "dipper_navy_bighash_bucket_bf_size"};
constexpr folly::StringPiece kSmallItemMaxSize{
    "dipper_navy_small_item_max_size"};
constexpr folly::StringPiece kMaxConcurrentInserts{
    "dipper_navy_max_concurrent_inserts"};
constexpr folly::StringPiece kMaxParcelMemoryMB{
    "dipper_navy_max_parcel_memory_mb"};
constexpr folly::StringPiece kCleanRegions{"dipper_navy_clean_regions"};
constexpr folly::StringPiece kReaderThreads{"dipper_navy_reader_threads"};
constexpr folly::StringPiece kWriterThreads{"dipper_navy_writer_threads"};
constexpr folly::StringPiece kReinsertionHitsThreshold{
    "dipper_navy_reinsertion_hits_threshold"};
constexpr folly::StringPiece kReinsertionProbabilityThreshold{
    "dipper_navy_reinsertion_probability_threshold"};
constexpr folly::StringPiece kNavyRequestOrderingShards{
    "dipper_navy_req_order_shards_power"};
constexpr folly::StringPiece kNumInMemBuffers{"dipper_navy_num_in_mem_buffers"};
constexpr folly::StringPiece kNavyDataChecksum{"dipper_navy_data_checksum"};

uint64_t megabytesToBytes(uint64_t mb) { return mb << 20; }

// Return a number that's equal or smaller than @num and aligned on @alignment
uint64_t alignDown(uint64_t num, uint64_t alignment) {
  return num - num % alignment;
}

// Return a number that's equal or bigger than @num and aligned on @alignment
uint64_t alignUp(uint64_t num, uint64_t alignment) {
  return alignDown(num + alignment - 1, alignment);
}

bool usesSimpleFile(const folly::dynamic& options) {
  auto fileName = options.get_ptr(kFileName);
  return fileName && !fileName->getString().empty();
}

bool usesRaidFiles(const folly::dynamic& options) {
  auto raidPaths = options.get_ptr(kRAIDPaths);
  return raidPaths && (raidPaths->size() > 0);
}

// Open cache file @fileName and set it size to @size.
// Throws std::system_error if failed.
folly::File openCacheFile(const std::string& fileName,
                          uint64_t size,
                          bool directIO,
                          bool truncate) {
  XLOG(ERR) << "Cache file: " << fileName << " size: " << size
            << " direct IO: " << directIO << " truncate: " << truncate;
  if (fileName.empty()) {
    throw std::invalid_argument("File name is empty");
  }
  int flags{O_RDWR | O_CREAT};
  if (directIO) {
    flags |= O_DIRECT;
  }
  auto f = folly::File(fileName.c_str(), flags);
  // TODO detect if file exists and is of expected size. If not,
  // automatically fallocate the file or ftruncate the file.
  if (truncate && ::fallocate(f.fd(), 0, 0, size) < 0) {
    throw std::system_error(
        errno,
        std::system_category(),
        folly::sformat("failed fallocate with size {}", size));
  }

  if (directIO && ::posix_fadvise(f.fd(), 0, size, POSIX_FADV_DONTNEED) < 0) {
    throw std::system_error(
        errno, std::system_category(), "Error fadvising cache file");
  }

  return f;
}

std::unique_ptr<cachelib::navy::Device> createDevice(
    const folly::dynamic& options) {
  const auto size = getNvmCacheSize(options);
  if (usesRaidFiles(options) && usesSimpleFile(options)) {
    throw std::invalid_argument("Can't use raid and simple file together");
  }

  if (usesRaidFiles(options) && options.get_ptr(kRAIDPaths)->size() <= 1) {
    throw std::invalid_argument("Raid needs more than one path");
  }
  if (usesRaidFiles(options)) {
    auto raidPaths = options.get_ptr(kRAIDPaths);

    // File paths are opened in the increasing order of the
    // path string. This ensures that RAID0 stripes aren't
    // out of order even if the caller changes the order of
    // the file paths. We can recover the cache as long as all
    // the paths are specified, regardless of the order.
    std::vector<std::string> paths;
    for (const auto& file : *raidPaths) {
      paths.push_back(file.getString());
    }
    std::sort(paths.begin(), paths.end());

    std::vector<int> fdvec;
    for (const auto& path : paths) {
      folly::File f;
      try {
        f = openCacheFile(path,
                          options[kFileSize].getInt(),
                          options[kDirectIO].getBool(),
                          options[kTruncateFile].getBool());
      } catch (const std::exception& e) {
        XLOG(ERR) << "Exception in openCacheFile: " << path << e.what();
        throw;
      }
      fdvec.push_back(f.release());
    } // for
    return cachelib::navy::createDirectIoRAID0Device(
        fdvec, options[kBlockSize].getInt(), options[kRegionSize].getInt());
  }

  if (!usesSimpleFile(options)) {
    return cachelib::navy::createMemoryDevice(size);
  }

  // Create a simple file device
  auto fileName = options.get_ptr(kFileName);
  folly::File f;
  try {
    f = openCacheFile(fileName->getString(),
                      size,
                      options[kDirectIO].getBool(),
                      options[kTruncateFile].getBool());
  } catch (const std::exception& e) {
    XLOG(ERR) << "Exception in openCacheFile: " << e.what();
    throw;
  }
  return cachelib::navy::createDirectIoFileDevice(f.release(),
                                                  options[kBlockSize].getInt());
}

void setupCacheProtos(const folly::dynamic& options,
                      cachelib::navy::CacheProto& proto) {
  const uint64_t totalCacheSize = getNvmCacheSize(options);

  auto blockSize = options[kBlockSize].getInt();
  auto getDefaultMetadataSize = [](size_t size, size_t alignment) {
    XDCHECK(folly::isPowTwo(alignment));
    auto mask = ~(alignment - 1);
    return (static_cast<size_t>(kDefaultMetadataPercent * size / 100) & mask);
  };
  uint64_t metadataSize =
      options
          .getDefault(kDeviceMetadataSize,
                      getDefaultMetadataSize(totalCacheSize, blockSize))
          .getInt();
  metadataSize = alignUp(metadataSize, blockSize);

  if (metadataSize >= totalCacheSize) {
    throw std::invalid_argument{
        folly::sformat("Invalid metadata size: {}. Cache size: {}",
                       metadataSize,
                       totalCacheSize)};
  }
  proto.setMetadataSize(metadataSize);
  uint64_t blockCacheSize = 0;
  const auto bigHashPctSize = options.get_ptr(kBigHashSizePct);
  if (bigHashPctSize && bigHashPctSize->getInt() > 0) {
    const auto bucketSize = options[kBigHashBucketSize].getInt();

    // If enabled, BigHash's storage starts after BlockCache's.
    const auto sizeReservedForBigHash =
        totalCacheSize * bigHashPctSize->getInt() / 100ul;

    const uint64_t bigHashCacheOffset =
        alignUp(totalCacheSize - sizeReservedForBigHash, bucketSize);
    const uint64_t bigHashCacheSize =
        alignDown(totalCacheSize - bigHashCacheOffset, bucketSize);

    auto bigHash = cachelib::navy::createBigHashProto();
    bigHash->setLayout(bigHashCacheOffset, bigHashCacheSize, bucketSize);

    // Bucket Bloom filter size, bytes
    const auto bfSize = options.getDefault(kBigHashBucketBFSize, 8).getInt();
    if (bfSize > 0) {
      // We set 4 hash function unconditionally. This seems to be the best
      // for our use case. If BF size to bucket size ratio gets lower, try
      // to reduce number of hashes.
      constexpr uint32_t kNumHashes = 4;
      const uint32_t bitsPerHash = bfSize * 8 / kNumHashes;
      bigHash->setBloomFilter(kNumHashes, bitsPerHash);
    }

    proto.setBigHash(std::move(bigHash), options[kSmallItemMaxSize].getInt());

    if (bigHashCacheOffset <= metadataSize) {
      throw std::invalid_argument("NVM cache size is not big enough!");
    }
    blockCacheSize = bigHashCacheOffset - metadataSize;
    XLOG(ERR) << "metadataSize: " << metadataSize
              << " blockCacheSize: " << blockCacheSize
              << " bigHashCacheOffset: " << bigHashCacheOffset
              << " bigHashCacheSize: " << bigHashCacheSize;
  } else {
    blockCacheSize = totalCacheSize - metadataSize;
    XLOG(ERR) << "metadataSize: " << metadataSize
              << " blockCacheSize: " << blockCacheSize << ". No Bighash";
  }

  if (blockCacheSize > 0) {
    auto blockCache = cachelib::navy::createBlockCacheProto();
    blockCache->setBlockSize(options[kBlockSize].getInt());
    blockCache->setLayout(
        metadataSize, blockCacheSize, options[kRegionSize].getInt());
    bool dataChecksum = options.getDefault(kNavyDataChecksum, true).getBool();
    blockCache->setChecksum(dataChecksum);
    if (options[kLru].getBool()) {
      blockCache->setLruEvictionPolicy();
    } else {
      blockCache->setFifoEvictionPolicy();
    }
    if (options.get_ptr(kSizeClasses) && !options[kSizeClasses].empty()) {
      std::vector<uint32_t> sizeClasses;
      for (const auto& sc : options[kSizeClasses]) {
        sizeClasses.push_back(sc.getInt());
      }
      DCHECK(!sizeClasses.empty());
      blockCache->setSizeClasses(std::move(sizeClasses));
    } else {
      blockCache->setReadBufferSize(options[kReadBuffer].getInt());
    }
    blockCache->setCleanRegionsPool(
        options.getDefault(kCleanRegions, 1).getInt());

    const uint8_t reinsertionHitsThreshold =
        options.getDefault(kReinsertionHitsThreshold, 0).getInt();
    if (reinsertionHitsThreshold > 0) {
      blockCache->setHitsReinsertionPolicy(reinsertionHitsThreshold);
    }

    const uint32_t reinsertionProbabilityThreshold =
        options.getDefault(kReinsertionProbabilityThreshold, 0).getInt();
    if (reinsertionProbabilityThreshold > 0) {
      blockCache->setProbabilisticReinsertionPolicy(
          reinsertionProbabilityThreshold);
    }

    blockCache->setNumInMemBuffers(
        options.getDefault(kNumInMemBuffers, 0).getInt());

    proto.setBlockCache(std::move(blockCache));
  }
}

void setAdmissionPolicy(const folly::dynamic& options,
                        cachelib::navy::CacheProto& proto) {
  auto policyName = options.get_ptr(kAdmissionPolicy);
  if (!policyName) {
    return;
  }
  const std::string& name = policyName->getString();
  if (name == "random") {
    proto.setRejectRandomAdmissionPolicy(options[kAdmissionProb].getDouble());
  } else if (name == "dynamic_random") {
    size_t admissionSuffixLen = options.get_ptr(kAdmissionSuffixLen)
                                    ? options[kAdmissionSuffixLen].getInt()
                                    : 0;
    proto.setDynamicRandomAdmissionPolicy(options[kAdmissionWriteRate].getInt(),
                                          admissionSuffixLen);
  } else {
    throw std::invalid_argument{folly::sformat("invalid policy name {}", name)};
  }
}

std::unique_ptr<cachelib::navy::JobScheduler> createJobScheduler(
    const folly::dynamic& options) {
  auto legacyDipperAsyncThreads =
      options.getDefault("dipper_async_threads", 32).getInt();
  auto readerThreads =
      options.getDefault(kReaderThreads, legacyDipperAsyncThreads).getInt();
  auto writerThreads =
      options.getDefault(kWriterThreads, legacyDipperAsyncThreads).getInt();
  if (options.get_ptr(kNavyRequestOrderingShards)) {
    auto reqOrderShardsPower = options[kNavyRequestOrderingShards].getInt();
    return std::make_unique<cachelib::navy::OrderedThreadPoolJobScheduler>(
        readerThreads, writerThreads, reqOrderShardsPower);
  } else {
    return std::make_unique<cachelib::navy::ThreadPoolJobScheduler>(
        readerThreads, writerThreads);
  }
}

} // namespace

uint64_t getNvmCacheSize(const folly::dynamic& options) {
  if (usesRaidFiles(options)) {
    auto raidPaths = options.get_ptr(kRAIDPaths);
    return raidPaths->size() * options[kFileSize].getInt();
  }
  // Simple file or memory
  return options[kFileSize].getInt();
}

std::unique_ptr<navy::AbstractCache> createNavyCache(
    const folly::dynamic& options, navy::DestructorCallback cb, bool truncate) {
  auto proto = cachelib::navy::createCacheProto();
  proto->setDevice(createDevice(options));
  proto->setJobScheduler(createJobScheduler(options));
  proto->setMaxConcurrentInserts(
      options.getDefault(kMaxConcurrentInserts, 1'000'000).getInt());
  proto->setMaxParcelMemory(
      megabytesToBytes(options.getDefault(kMaxParcelMemoryMB, 256).getInt()));
  setAdmissionPolicy(options, *proto);
  proto->setDestructorCallback(cb);

  setupCacheProtos(options, *proto);

  auto cache = createCache(std::move(proto));
  XDCHECK(cache != nullptr);

  if (truncate) {
    cache->reset();
    return cache;
  }

  if (!cache->recover()) {
    XLOG(ERR) << "No recovery data found. Continuing with clean cache.";
  }
  return cache;
}

void populateDefaultNavyOptions(folly::dynamic& options) {
  // default values for some of the options if they are not already present.
  folly::dynamic defs = folly::dynamic::object;
  defs[kDirectIO] = true;
  defs[kBlockSize] = 4096;
  defs[kRegionSize] = 16 * 1024 * 1024;
  defs[kLru] = true;
  defs[kReadBuffer] = 4096;
  defs[kTruncateFile] = false;
  defs[kBigHashBucketBFSize] = 8;
  defs[kRAIDPaths] = folly::dynamic::array;

  options.update_missing(defs);
  return;
}

} // namespace cachelib
} // namespace facebook

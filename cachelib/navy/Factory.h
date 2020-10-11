#pragma once

#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/block_cache/ReinsertionPolicy.h"
#include "cachelib/navy/common/Device.h"

#include "cachelib/navy/scheduler/JobScheduler.h"

#include <chrono>
#include <memory>
#include <vector>

namespace facebook {
namespace cachelib {
namespace navy {

// *Proto class convention:
// Every method must be called no more than once and may throw std::exception
// (or derived) if parameters are invalid.

// Block Cache (BC) engine proto. BC is used to cache medium size objects
// (1Kb - 512Kb typically). User sets up BC parameters and passes proto
// to CacheProto::setBlockCache.
class BlockCacheProto {
 public:
  virtual ~BlockCacheProto() = default;

  // Set cache layout. Cache will start at @baseOffset and will be @size bytes
  // on the device. @regionSize is the region size (bytes).
  virtual void setLayout(uint64_t baseOffset,
                         uint64_t size,
                         uint32_t regionSize) = 0;

  // Enable data checksumming (default: disabled)
  virtual void setChecksum(bool enable) = 0;

  // set*EvictionPolicy function family: sets eviction policy. Supports LRU,
  // LRU with deferred insert and FIFO. Must set up one of them.

  // Sets LRU eviction policy.
  virtual void setLruEvictionPolicy() = 0;

  // Sets FIFO eviction policy.
  virtual void setFifoEvictionPolicy() = 0;

  // Sets SegmentedFIFO eviction policy.
  // @segmentRatio  ratio of the size of each segment.
  virtual void setSegmentedFifoEvictionPolicy(
      std::vector<unsigned int> segmentRatio) = 0;

  // (Optional) Size classes list. Stack allocator used if not set.
  virtual void setSizeClasses(std::vector<uint32_t> sizeClasses) = 0;

  // (Optional) In case of stack alloc, determines recommended size of the
  // read buffer. Must be multiple of block size.
  virtual void setReadBufferSize(uint32_t size) = 0;

  // (Optional) How many clean regions GC should (try to) maintain in the pool.
  // Default: 1
  virtual void setCleanRegionsPool(uint32_t n) = 0;

  // (Optional) Number of In memory buffers to maintain. Default: 0
  virtual void setNumInMemBuffers(uint32_t numInMemBuffers) = 0;

  // (Optional) Enable hits reinsertion policy that determines if an item should
  // stay for more time in cache. @reinsertionThreshold means if an item had
  // been accessed more than that threshold, it will be eligible for
  // reinsertion.
  virtual void setHitsReinsertionPolicy(uint8_t reinsertionThreshold) = 0;

  // (Optional) Enable probability reinsertion policy that determines if an item
  // should stay for more time in cache. @probablity lets user specify a
  // probability between 0 and 100 for reinsertion.
  virtual void setProbabilisticReinsertionPolicy(uint32_t probability) = 0;
};

// BigHash engine proto. BigHash is used to cache small objects (under 2KB)
// User sets up this proto object and passes it to CacheProto::setBigHash.
class BigHashProto {
 public:
  virtual ~BigHashProto() = default;

  // Set cache layout. Cache will start at @baseOffset and will be @size bytes
  // on the device. BigHash divides its device spcae into a number of fixed size
  // buckets, represented by @bucketSize. All IO happens on bucket-size
  // granularity.
  virtual void setLayout(uint64_t baseOffset,
                         uint64_t size,
                         uint32_t bucketSize) = 0;

  // Enable Bloom filter with @numHashes hash functions, each mapped into an
  // bit array of @hashTableBitSize bits.
  virtual void setBloomFilter(uint32_t numHashes,
                              uint32_t hashTableBitSize) = 0;
};

// Cache object prototype. Setup cache desired parameters and pass proto to
// @createCache function.
class CacheProto {
 public:
  virtual ~CacheProto() = default;

  // Set maximum concurrent insertions allowed in the driver.
  virtual void setMaxConcurrentInserts(uint32_t limit) = 0;

  // Set maximum parcel memory for all queues inserts. Parcel is a buffer with
  // key and value.
  virtual void setMaxParcelMemory(uint64_t limit) = 0;

  // Sets device that engine will use.
  virtual void setDevice(std::unique_ptr<Device> device) = 0;

  // Sets metadata size.
  virtual void setMetadataSize(size_t metadataSize) = 0;

  // Set up block cache engine.
  virtual void setBlockCache(std::unique_ptr<BlockCacheProto> proto) = 0;

  // Set up big hash engine.
  virtual void setBigHash(std::unique_ptr<BigHashProto> proto,
                          uint32_t smallItemMaxSize) = 0;

  // Set JobScheduler for async function calls.
  virtual void setJobScheduler(std::unique_ptr<JobScheduler> ex) = 0;

  // (Optional) Set destructor callback.
  //   - Callback invoked exactly once for every insert, even if it was removed
  //     manually from the cache with @AbstractCache::remove.
  //   - If a key was removed manually, the DestructorEvent will be
  //     DestructorEvent::Removed. If it was evicted, DestructorEvent::Recycled
  //   - No any guarantees on order, even for a specific key.
  //   - No any guarantees on timing. If entry was removed/evicted, we only
  //     guarantee to invoke the callback at some point of time.
  //   - Callback should be lightweight.
  virtual void setDestructorCallback(DestructorCallback cb) = 0;

  // (Optional) Set admission policy to accept a random item with specified
  // probability.
  virtual void setRejectRandomAdmissionPolicy(double probability) = 0;

  // (Optional) Set admission policy to accept a random item with a target write
  // rate in bytes/s. setBlockCache is a dependency and must be called before
  // this.
  virtual void setDynamicRandomAdmissionPolicy(
      uint64_t targetRate, size_t deterministicKeyHashSuffixLength = 0) = 0;
};

std::unique_ptr<BlockCacheProto> createBlockCacheProto();
std::unique_ptr<BigHashProto> createBigHashProto();
std::unique_ptr<CacheProto> createCacheProto();
std::unique_ptr<AbstractCache> createCache(std::unique_ptr<CacheProto> proto);
} // namespace navy
} // namespace cachelib
} // namespace facebook

/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/CompilerUtils.h"
#include "cachelib/navy/block_cache/Allocator.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/block_cache/PercentageReinsertionPolicy.h"
#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/SizeDistribution.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
class JobScheduler;

// Constructor can throw but system remains in the valid state. Caller can
// fix parameters and re-run.
class BlockCache final : public Engine {
 public:
  // See CacheProto for details
  struct Config {
    Device* device{};
    ExpiredCheck checkExpired;
    DestructorCallback destructorCb;
    // Checksum data read/written
    bool checksum{};
    // Base offset and size (in bytes) of cache on the device
    uint64_t cacheBaseOffset{};
    uint64_t cacheSize{};
    // Eviction policy
    std::unique_ptr<EvictionPolicy> evictionPolicy;
    BlockCacheReinsertionConfig reinsertionConfig{};
    // Region size, bytes
    uint64_t regionSize{16 * 1024 * 1024};
    // See AbstractCacheProto::setReadBufferSize
    uint32_t readBufferSize{};
    // Job scheduler for background tasks
    JobScheduler* scheduler{};
    // Clean region pool size
    uint32_t cleanRegionsPool{1};
    // Number of in-memory buffers where writes are buffered before flushed
    // on to the device
    uint32_t numInMemBuffers{1};
    // whether ItemDestructor is enabled
    bool itemDestructorEnabled{false};

    // Maximum number of retry times for in-mem buffer flushing.
    // When exceeding the limit, we will not reschedule any flushing job but
    // directly fail it.
    uint16_t inMemBufFlushRetryLimit{10};

    // Number of priorities. Items of the same priority will be put into
    // the same reigon. The effect of priorities will be up to the particular
    // eviction policy. There must be at least one priority.
    uint16_t numPriorities{1};

    // whether to remove an item by checking the full key.
    bool preciseRemove{false};

    // Calculates the total region number.
    uint32_t getNumRegions() const {
      XDCHECK_EQ(0ul, cacheSize % regionSize);
      return cacheSize / regionSize;
    }

    // Checks invariants. Throws exception if failed.
    Config& validate();
  };

  // Contructor can throw std::exception if config is invalid.
  //
  // @param config  config that was validated with Config::validate
  //
  // @throw std::invalid_argument on bad config
  explicit BlockCache(Config&& config);
  BlockCache(const BlockCache&) = delete;
  BlockCache& operator=(const BlockCache&) = delete;
  ~BlockCache() override = default;

  // return the size of usable space
  uint64_t getSize() const override { return regionManager_.getSize(); }

  // Checks if the key could exist in block cache. This can be used as a
  // pre-check to optimize cache lookups to avoid calling lookup in an async IO
  // environment.
  //
  // @param hk   key to be checked
  //
  // @return  false if the key definitely does not exist and true if it could.
  bool couldExist(HashedKey hk) override;

  // Inserts a key-value pair into BlockCache.
  //
  // @param hk      key to be inserted
  // @param value   value to be inserted
  //
  // @return  Status::Ok on success,
  //          Status::Rejected on error,
  //          Status::Retry on no space available for now.
  Status insert(HashedKey hk, BufferView value) override;

  // Looks up a key in BlockCache.
  //
  // @param hk      key to be looked up
  // @param value   value to be populated with the data read
  //
  // @return  Status::Ok on success,
  //          Status::NotFound if the key is not found,
  //          Status::Retry read cannot be served and needs to be retried again.
  //          Status::DeviceError otherwise.
  Status lookup(HashedKey hk, Buffer& value) override;

  // Removes a key from BlockCache.
  //
  // @param hk           key to be removed
  //
  // @return Status::Ok if the key is found and Status::NotFound otherwise.
  Status remove(HashedKey hk) override;

  // Flushes all buffered (in flight) operations in BlockCache.
  void flush() override;

  // Resets BlockCache to its initial state.
  void reset() override;

  // Serializes BlockCache state to a RecordWriter.
  //
  // @param rw   RecordWriter to serialize state
  void persist(RecordWriter& rw) override;

  // Deserialize BlockCache state from a RecordReader.
  //
  // @param rr   RecordReader to deserialize state
  //
  // @return  true if recovery succeeds, false otherwise.
  bool recover(RecordReader& rr) override;

  // Exports BlockCache stats via CounterVisitor.
  //
  // @param visitor   CounterVisitor to export stats
  void getCounters(const CounterVisitor& visitor) const override;

  // Gets the maximum item size that can be inserted into BlockCache.
  uint64_t getMaxItemSize() const override {
    return regionSize_ - sizeof(EntryDesc);
  }

  // Gets the alloc alignment size (must be an integral power of two).
  uint32_t getAllocAlignSize() const {
    XDCHECK(folly::isPowTwo(allocAlignSize_));
    return allocAlignSize_;
  }

  // Return a Buffer containing NvmItem randomly sampled in the backing store
  std::pair<Status, std::string /* key */> getRandomAlloc(
      Buffer& value) override;

  // The minimum alloc alignment size can be as small as 1. Since the
  // test cases have very small device size, they will end up with alloc
  // alignment size of 1 (if determined as device_size >> 32 ) and we may
  // not be able to test the "alloc alignment size" feature correctly.
  // Since the real devices are not going to be that small, this is a
  // reasonable minimum size. This can be lowered to 256 or 128 in future,
  // if needed.
  static constexpr uint32_t kMinAllocAlignSize = 512;

  // This is the maximum size of an item that can be cached in block cache.
  static constexpr uint32_t kMaxItemSize =
      kMinAllocAlignSize *
      static_cast<uint32_t>(std::numeric_limits<uint16_t>::max());

 private:
  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 12;
  // This should be at least the nextTwoPow(sizeof(EntryDesc)).
  static constexpr uint32_t kDefReadBufferSize = 4096;
  // Default priority for an item inserted into block cache
  static constexpr uint16_t kDefaultItemPriority = 0;

  // When modify @EntryDesc layout, don't forget to bump @kFormatVersion!
  struct EntryDesc {
    uint32_t keySize{};
    uint32_t valueSize{};
    uint64_t keyHash{};
    uint32_t csSelf{};
    uint32_t cs{};

    EntryDesc() = default;
    EntryDesc(uint32_t ks, uint32_t vs, uint64_t kh)
        : keySize{ks}, valueSize{vs}, keyHash{kh} {
      csSelf = computeChecksum();
    }

    uint32_t computeChecksum() const {
      return checksum(BufferView{offsetof(EntryDesc, csSelf),
                                 reinterpret_cast<const uint8_t*>(this)});
    }
  };

  // Instead of unportable packing, we make sure that struct size is equal to
  // the size of its members.
  static_assert(sizeof(EntryDesc) == 24, "packed struct required");

  struct ValidConfigTag {};
  BlockCache(Config&& config, ValidConfigTag);

  // Entry disk size (with aux data and aligned)
  uint32_t serializedSize(uint32_t keySize, uint32_t valueSize);

  // Read and write are time consuming. It doesn't worth inlining them from
  // the performance point of view, but makes sense to track them for perf:
  // especially portion on CPU time spent in std::memcpy.
  // @param addr        Address to write this entry into
  // @param slotSize    Number of bytes this entry will take up on the device
  // @param hk          Key of the entry
  // @param value       Payload of the entry
  Status writeEntry(RelAddress addr,
                    uint32_t slotSize,
                    HashedKey hk,
                    BufferView value);
  // @param readDesc      Descriptor for reading. This must be valid
  // @param addrEnd       End of the entry since the item layout is backward
  // @param approxSize    Approximate size since we got this size from index
  // @param expected      We expect the entry's key to match with our key
  // @param value         We will write the payload into this buffer
  Status readEntry(const RegionDescriptor& readDesc,
                   RelAddress addrEnd,
                   uint32_t approxSize,
                   HashedKey expected,
                   Buffer& value);

  // Allocator reclaim callback
  // Returns number of slots that were successfully evicted
  uint32_t onRegionReclaim(RegionId rid, BufferView buffer);

  // Allocator cleanup callback
  void onRegionCleanup(RegionId rid, BufferView buffer);

  // Returns true if @config matches this cache's config_
  bool isValidRecoveryData(const serialization::BlockCacheConfig& config) const;

  static serialization::BlockCacheConfig serializeConfig(const Config& config);

  // Tries to recover cache. Throws std::exception on failure.
  void tryRecover(RecordReader& rr);

  // The alloc alignment indicates the granularity of read/write. This
  // granuality is less than the device io alignment size because we buffer
  // writes in memory until we fill up a region.
  uint32_t calcAllocAlignSize() const;

  // Size hint is computed by aligning size up to kMinAllocAlignSize,
  // and then divide by it. It is loosely compressing the size as
  // we may decode into a bigger size later.
  uint16_t encodeSizeHint(uint32_t size) const {
    auto alignedSize = powTwoAlign(size, kMinAllocAlignSize);
    return static_cast<uint16_t>(alignedSize / kMinAllocAlignSize);
  }

  uint32_t decodeSizeHint(uint16_t sizeHint) const {
    return sizeHint * kMinAllocAlignSize;
  }

  uint32_t encodeRelAddress(RelAddress addr) const {
    XDCHECK_NE(addr.offset(), 0u); // See @decodeRelAddress
    return regionManager_.toAbsolute(addr).offset() / allocAlignSize_;
  }

  AbsAddress decodeAbsAddress(uint32_t code) const {
    return AbsAddress{static_cast<uint64_t>(code) * allocAlignSize_};
  }

  RelAddress decodeRelAddress(uint32_t code) const {
    // Remember, we store slot end address, which can have offset equal to
    // region size.
    return regionManager_.toRelative(decodeAbsAddress(code).sub(1)).add(1);
  }

  enum class ReinsertionRes {
    // Item was reinserted back into the cache
    kReinserted,
    // Item was removed by user earlier
    kRemoved,
    // Item wasn't eligible for re-insertion and was evicted
    kEvicted,
  };
  ReinsertionRes reinsertOrRemoveItem(HashedKey hk,
                                      BufferView value,
                                      uint32_t entrySize,
                                      RelAddress currAddr);

  // Removes an entry key from the index.
  // @return true if the item is successfully removed; false if the item cannot
  //         be found or was removed earlier.
  bool removeItem(HashedKey hk, RelAddress currAddr);

  void validate(Config& config) const;

  // Create the reinsertion policy from config.
  // This function may need a reference to index and should be called the last
  // in the initialization order.
  std::shared_ptr<BlockCacheReinsertionPolicy> makeReinsertionPolicy(
      const BlockCacheReinsertionConfig& reinsertionConfig);

  const serialization::BlockCacheConfig config_;
  const uint16_t numPriorities_{};
  const ExpiredCheck checkExpired_;
  const DestructorCallback destructorCb_;
  const bool checksumData_{};
  // reference to the under-lying device.
  const Device& device_;
  // alloc alignment size indicates the granularity of entry sizes on device.
  // this is at least kMinAllocAlignSize and is determined by the size of the
  // device and size of the address (which is 32-bits).
  const uint32_t allocAlignSize_{};
  const uint32_t readBufferSize_{};
  // number of bytes in a region
  const uint64_t regionSize_{};
  // whether ItemDestructor is enabled
  const bool itemDestructorEnabled_{false};
  // whether preciseRemove is enabled
  const bool preciseRemove_{false};

  // Index stores offset of the slot *end*. This enables efficient paradigm
  // "buffer pointer is value pointer", which means value has to be at offset 0
  // of the slot and header (footer) at the end.
  //
  // -------------------------------------------
  // |     Value                    |  Footer  |
  // -------------------------------------------
  // ^                                         ^
  // |                                         |
  // Buffer*                          Index points here
  Index index_;
  RegionManager regionManager_;
  Allocator allocator_;
  // It is vital that the reinsertion policy is initialized after index_.
  // Make sure that this class member is defined after index_.
  std::shared_ptr<BlockCacheReinsertionPolicy> reinsertionPolicy_;

  // thread local counters in synchronized/critical path
  mutable TLCounter lookupCount_;
  mutable TLCounter succLookupCount_;

  // atomic counters in asynchronized path
  mutable AtomicCounter insertCount_;
  mutable AtomicCounter insertHashCollisionCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupFalsePositiveCount_;
  mutable AtomicCounter lookupEntryHeaderChecksumErrorCount_;
  mutable AtomicCounter lookupValueChecksumErrorCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionLookupMissCounter_;
  mutable AtomicCounter evictionExpiredCount_;
  mutable AtomicCounter allocErrorCount_;
  mutable AtomicCounter logicalWrittenCount_;
  // TODO: deprecate hole count and hole size when we have
  //       confirmed usedSizeBytes is working correctly in prod
  mutable AtomicCounter holeCount_;
  mutable AtomicCounter holeSizeTotal_;
  mutable AtomicCounter usedSizeBytes_;
  mutable AtomicCounter reinsertionErrorCount_;
  mutable AtomicCounter reinsertionCount_;
  mutable AtomicCounter reinsertionBytes_;
  mutable AtomicCounter reclaimEntryHeaderChecksumErrorCount_;
  mutable AtomicCounter reclaimValueChecksumErrorCount_;
  mutable AtomicCounter removeAttemptCollisions_;
  mutable AtomicCounter cleanupEntryHeaderChecksumErrorCount_;
  mutable AtomicCounter cleanupValueChecksumErrorCount_;
  mutable AtomicCounter lookupForItemDestructorErrorCount_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

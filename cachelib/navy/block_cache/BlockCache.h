#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/block_cache/Allocator.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/block_cache/ReinsertionPolicy.h"
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
    Device* device;
    DestructorCallback destructorCb;
    // Checksum data read/written
    bool checksum{false};
    // Device block size, bytes
    uint32_t blockSize{1024};
    // Base offset and size (in bytes) of cache on the device
    uint64_t cacheBaseOffset{};
    uint64_t cacheSize{};
    // Eviction policy
    std::unique_ptr<EvictionPolicy> evictionPolicy;
    // reinsertion policy
    std::unique_ptr<ReinsertionPolicy> reinsertionPolicy;
    // Sorted list of size classes (empty means stack allocator)
    std::vector<uint32_t> sizeClasses;
    // Region size, bytes
    uint64_t regionSize{16 * 1024 * 1024};
    // See AbstractCacheProto::setReadBufferSize
    uint32_t readBufferSize{0};
    // Job scheduler for background tasks
    JobScheduler* scheduler{nullptr};
    // Clean region pool size
    uint32_t cleanRegionsPool{1};

    uint32_t getNumRegions() const { return cacheSize / regionSize; }

    uint32_t getReadBufferSize() const {
      return readBufferSize ? readBufferSize : blockSize;
    }

    // Checks invariants. Throws exception if failed.
    Config& validate();
  };

  explicit BlockCache(Config&& config);
  BlockCache(const BlockCache&) = delete;
  BlockCache& operator=(const BlockCache&) = delete;
  ~BlockCache() override = default;

  Status insert(HashedKey hk, BufferView value, InsertOptions opt) override;
  Status lookup(HashedKey hk, Buffer& value) override;
  Status remove(HashedKey hk) override;

  void flush() override;
  void reset() override;

  void persist(RecordWriter& rw) override;
  bool recover(RecordReader& rr) override;

  void getCounters(const CounterVisitor& visitor) const override;

 private:
  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 11;

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
  static uint32_t serializedSize(uint32_t keySize, uint32_t valueSize);

  // Read and write are time consuming. It doesn't worth inlining them from
  // the performance point of view, but makes sense to track them for perf:
  // especially portion on CPU time spent in std::memcpy.
  Status writeEntry(RelAddress addr,
                    uint32_t slotSize,
                    HashedKey hk,
                    BufferView value);
  Status readEntry(RelAddress addrEnd, HashedKey expected, Buffer& value);

  // Allocator reclaim callback
  // Returns number of slots that were successfully evicted
  uint32_t onRegionReclaim(RegionId rid, uint32_t slotSize, BufferView buffer);

  // Returns true if @config matches this cache's config_
  bool isValidRecoveryData(const serialization::BlockCacheConfig& config) const;

  static serialization::BlockCacheConfig serializeConfig(const Config& config);

  // Tries to recover cache. Throws std::exception on failure.
  void tryRecover(RecordReader& rr);

  uint32_t encodeRelAddress(RelAddress addr) const {
    XDCHECK_NE(addr.offset(), 0u); // See @decodeRelAddress
    return regionManager_.toAbsolute(addr).offset() / blockSize_;
  }

  AbsAddress decodeAbsAddress(uint32_t code) const {
    return AbsAddress{code * blockSize_};
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

  const serialization::BlockCacheConfig config_;
  const DestructorCallback destructorCb_;
  const bool checksumData_{};
  // Must be 64 bits because we multiply 32 bit block offset by it to get
  // 64 bit byte offset.
  const uint64_t blockSize_{};
  const uint32_t readBufferSize_{};

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
  std::unique_ptr<ReinsertionPolicy> reinsertionPolicy_;

  mutable AtomicCounter insertCount_;
  mutable AtomicCounter insertHashCollisionCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter lookupFalsePositiveCount_;
  mutable AtomicCounter lookupChecksumErrorCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionLookupMissCounter_;
  mutable AtomicCounter allocErrorCount_;
  mutable AtomicCounter logicalWrittenCount_;
  mutable AtomicCounter holeCount_;
  mutable AtomicCounter holeSizeTotal_;
  mutable AtomicCounter reinsertionErrorCount_;
  mutable AtomicCounter reinsertionCount_;
  mutable AtomicCounter reinsertionBytes_;
  mutable SizeDistribution sizeDist_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

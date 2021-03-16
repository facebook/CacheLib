#pragma once

#include <chrono>
#include <stdexcept>

#include <folly/SharedMutex.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/BloomFilter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/SizeDistribution.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/kangaroo/LogBucket.h"
#include "cachelib/navy/kangaroo/KangarooLog.h"
#include "cachelib/navy/kangaroo/KangarooSizeDistribution.h"
#include "cachelib/navy/kangaroo/RripBitVector.h"
#include "cachelib/navy/kangaroo/RripBucket.h"
#include "cachelib/navy/kangaroo/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Kangaroo is a small item flash-based cache engine. It divides the device into
// a series of buckets. One can think of it as a on-device hash table.
//
// Each item is hashed to a bucket according to its key. There is no size class,
// and each bucket is consisted of various variable-sized items. When full, we
// evict the items in their insertion order. An eviction call back is guaranteed
// to be invoked once per item. We currently do not support removeCB. That is
// coming as part of Navy eventually.
//
// Each read and write via Kangaroo happens in `bucketSize` granularity. This
// means, you will read a full bucket even if your item is only 100 bytes.
// It's also the same for writes. This makes Kangaroo inherently unsuitable for
// large items that will also need large buckets (several KB and above).
//
// However, this design gives us the ability to forgo an in-memory index and
// instead look up our items directly from disk. In practice, this means Kangaroo
// is a flash engine optimized for small items.
class Kangaroo final : public Engine {
 public:
  struct Config {
    uint32_t bucketSize{4 * 1024};

    // The range of device that Kangaroo will access is guaranted to be
    // with in [baseOffset, baseOffset + cacheSize)
    uint64_t cacheBaseOffset{};
    uint64_t totalSetSize{};
    Device* device{nullptr};

    DestructorCallback destructorCb;

    // Optional bloom filter to reduce IO
    std::unique_ptr<BloomFilter> bloomFilter;

    std::unique_ptr<RripBitVector> rripBitVector;

    // Better to underestimate, used for pre-allocating log index
    // only needed for Kangaroo
    uint32_t avgSmallObjectSize{100};
    uint32_t logIndexPartitionsPerPhysical{};

    uint64_t numBuckets() const { return totalSetSize / bucketSize; }

    KangarooLog::Config logConfig;

    Config& validate();
  };

  // Throw std::invalid_argument on bad config
  explicit Kangaroo(Config&& config);

  ~Kangaroo() override = default;

  Kangaroo(const Kangaroo&) = delete;
  Kangaroo& operator=(const Kangaroo&) = delete;

  // Check if the key could exist in bighash. This can be used as a pre-check
  // to optimize cache lookups to avoid calling lookups in an async IO
  // environment.
  //
  // @param hk   key to be checked
  //
  // @return  false if the key definitely does not exist and true if it could.
  bool couldExist(HashedKey hk) override;
  // Look up a key in Kangaroo. On success, it will return Status::Ok and
  // populate "value" with the value found. User should pass in a null
  // Buffer as "value" as any existing storage will be freed. If not found,
  // it will return Status::NotFound. And of course, on error, it returns
  // DeviceError.
  Status lookup(HashedKey hk, Buffer& value) override;

  // Inserts key and value into Kangaroo. This will replace an existing
  // key if found. If it failed to write, it will return DeviceError.
  Status insert(HashedKey hk,
                BufferView value) override;

  // Removes an entry from Kangaroo if found. Ok on success, NotFound on miss,
  // and DeviceError on error.
  Status remove(HashedKey hk) override;

  void flush() override;

  void reset() override;

  void persist(RecordWriter& rw) override;
  bool recover(RecordReader& rr) override;

  void getCounters(const CounterVisitor& visitor) const override;

  uint64_t bfRejectCount() const { return bfRejectCount_.get(); }

 private:
  struct ValidConfigTag {};
  Kangaroo(Config&& config, ValidConfigTag);

  Buffer readBucket(KangarooBucketId bid);
  bool writeBucket(KangarooBucketId bid, Buffer buffer);

  // The corresponding r/w bucket lock must be held during the entire
  // duration of the read and write operations. For example, during write,
  // if write lock is dropped after a bucket is read from device, user
  // must re-acquire the write lock and re-read the bucket from device
  // again to ensure they have the newest content. Otherwise, one thread
  // could overwrite another's writes.
  //
  // In short, just hold the lock during the entire operation!
  folly::SharedMutex& getMutex(KangarooBucketId bid) const {
    return mutex_[bid.index() & (kNumMutexes - 1)];
  }

  KangarooBucketId getKangarooBucketId(HashedKey hk) const {
    return KangarooBucketId{static_cast<uint32_t>(hk.keyHash() % numBuckets_)};
  }

  KangarooBucketId getKangarooBucketIdFromHash(uint64_t hash) const {
    return KangarooBucketId{static_cast<uint32_t>(hash % numBuckets_)};
  }

  uint64_t getBucketOffset(KangarooBucketId bid) const {
    return cacheBaseOffset_ + bucketSize_ * bid.index();
  }

  double bfFalsePositivePct() const;
  void bfRebuild(KangarooBucketId bid, const RripBucket* bucket);
  bool bfReject(KangarooBucketId bid, uint64_t keyHash) const;

  bool bvGetHit(KangarooBucketId bid, uint32_t keyIdx) const;
  void bvSetHit(KangarooBucketId bid, uint32_t keyIdx) const;

  void insertMultipleObjectsToKangarooBucket(std::vector<std::unique_ptr<ObjectInfo>>& ois, 
      ReadmitCallback readmit);

  // Use birthday paradox to estimate number of mutexes given number of parallel
  // queries and desired probability of lock collision.
  static constexpr size_t kNumMutexes = 16 * 1024;

  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 10;
  
  // Open addressing index overhead
  static constexpr double LogIndexOverhead = 2;

  const DestructorCallback destructorCb_{};
  const uint64_t bucketSize_{};
  const uint64_t cacheBaseOffset_{};
  const uint64_t numBuckets_{};
  std::unique_ptr<BloomFilter> bloomFilter_;
  std::unique_ptr<RripBitVector> bitVector_;
  std::unique_ptr<KangarooLog> log_{nullptr};
  std::chrono::nanoseconds generationTime_{};
  Device& device_;
  std::unique_ptr<folly::SharedMutex[]> mutex_{
      new folly::SharedMutex[kNumMutexes]};
  mutable AtomicCounter itemCount_;
  mutable AtomicCounter logItemCount_;
  mutable AtomicCounter setItemCount_;
  mutable AtomicCounter insertCount_;
  mutable AtomicCounter logInsertCount_;
  mutable AtomicCounter setInsertCount_;
  mutable AtomicCounter readmitInsertCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter setHits_;
  mutable AtomicCounter logHits_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionCount_;
  mutable AtomicCounter logicalWrittenCount_;
  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter bfFalsePositiveCount_;
  mutable AtomicCounter bfProbeCount_;
  mutable AtomicCounter bfRejectCount_;
  mutable AtomicCounter checksumErrorCount_;
  mutable AtomicCounter thresholdNotHit_;
  mutable AtomicCounter multiInsertCalls_;
  mutable SizeDistribution sizeDist_;
  mutable KangarooSizeDistribution thresholdSizeDist_;
  mutable KangarooSizeDistribution thresholdNumDist_;

  static_assert((kNumMutexes & (kNumMutexes - 1)) == 0,
                "number of mutexes must be power of two");
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

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

#include <folly/fibers/TimedMutex.h>

#include <chrono>
#include <stdexcept>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/bighash/Bucket.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/SizeDistribution.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/engine/Engine.h"

namespace facebook {
namespace cachelib {
namespace navy {
// SharedMutex is write priority by default
using SharedMutex =
    folly::fibers::TimedRWMutexWritePriority<folly::fibers::Baton>;
// BigHash is a small item flash-based cache engine. It divides the device into
// a series of buckets. One can think of it as a on-device hash table.
//
// Each item is hashed to a bucket according to its key. There is no size class,
// and each bucket is consist of various variable-sized items. When full, we
// evict the items in their insertion order. An eviction call back is guaranteed
// to be invoked once per item. We currently do not support removeCB. That is
// coming as part of Navy eventually.
//
// Each read and write via BigHash happens in `bucketSize` granularity. This
// means, you will read a full bucket even if your item is only 100 bytes.
// It's also the same for writes. This makes BigHash inherently unsuitable for
// large items that will also need large buckets (several KB and above).
//
// However, this design gives us the ability to forgo an in-memory index and
// instead look up our items directly from disk. In practice, this means BigHash
// is a flash engine optimized for small items.
class BigHash final : public Engine {
 public:
  struct Config {
    uint32_t bucketSize{4 * 1024};

    // The range of device that BigHash will access is guaranteed to be
    // within [baseOffset, baseOffset + cacheSize)
    uint64_t cacheBaseOffset{};
    uint64_t cacheSize{};
    Device* device{nullptr};

    ExpiredCheck checkExpired;
    DestructorCallback destructorCb;

    // Optional bloom filter to reduce IO
    std::unique_ptr<BloomFilter> bloomFilter;

    uint64_t numBuckets() const { return cacheSize / bucketSize; }

    Config& validate();
  };

  // Contructor can throw std::exception if config is invalid.
  //
  // @param config  config that was validated with Config::validate
  //
  // @throw std::invalid_argument on bad config
  explicit BigHash(Config&& config);
  BigHash(const BigHash&) = delete;
  BigHash& operator=(const BigHash&) = delete;
  ~BigHash() override = default;

  // Return the size of usable space
  uint64_t getSize() const override { return bucketSize_ * numBuckets_; }

  // Check if the key could exist in bighash. This can be used as a pre-check
  // to optimize cache lookups to avoid calling lookups in an async IO
  // environment.
  //
  // @param hk   key to be checked
  //
  // @return  false if the key definitely does not exist and true if it could.
  bool couldExist(HashedKey hk) override;

  uint64_t estimateWriteSize(HashedKey, BufferView) const override;

  // Look up a key in BigHash. On success, it will return Status::Ok and
  // populate "value" with the value found. User should pass in a null
  // Buffer as "value" as any existing storage will be freed. If not found,
  // it will return Status::NotFound. And of course, on error, it returns
  // DeviceError.
  Status lookup(HashedKey hk, Buffer& value) override;

  // Inserts key and value into BigHash. This will replace an existing
  // key if found. If it failed to write, it will return DeviceError.
  Status insert(HashedKey hk, BufferView value) override;

  // Removes an entry from BigHash if found. Ok on success, NotFound on miss,
  // and DeviceError on error.
  Status remove(HashedKey hk) override;

  // flush the device file
  void flush() override;

  // reset BigHash, this clears the bloom filter and all stats
  // data is invalidated even it is not physically removed.
  void reset() override;

  // serialize BigHash state to a RecordWriter
  void persist(RecordWriter& rw) override;

  // deserialize BigHash state from a RecordReader
  // @return true if recovery succeed, false o/w.
  bool recover(RecordReader& rr) override;

  // returns BigHash stats to the visitor
  void getCounters(const CounterVisitor& visitor) const override;

  // return the maximum allowed item size
  uint64_t getMaxItemSize() const override;

  // return how manu times a lookup is rejected by the bloom filter
  uint64_t bfRejectCount() const { return bfRejectCount_.get(); }

  // return a Buffer containing NvmItem randomly sampled in the backing store
  std::pair<Status, std::string /* key */> getRandomAlloc(
      Buffer& value) override;

 private:
  class BucketId {
   public:
    explicit BucketId(uint32_t idx) : idx_{idx} {}

    bool operator==(const BucketId& rhs) const noexcept {
      return idx_ == rhs.idx_;
    }
    bool operator!=(const BucketId& rhs) const noexcept {
      return !(*this == rhs);
    }

    uint32_t index() const noexcept { return idx_; }

   private:
    uint32_t idx_;
  };

  struct ValidConfigTag {};
  BigHash(Config&& config, ValidConfigTag);

  Buffer readBucket(BucketId bid);
  bool writeBucket(BucketId bid, Buffer buffer);

  // The corresponding r/w bucket lock must be held during the entire
  // duration of the read and write operations. For example, during write,
  // if write lock is dropped after a bucket is read from device, user
  // must re-acquire the write lock and re-read the bucket from device
  // again to ensure they have the newest content. Otherwise, one thread
  // could overwrite another's writes.
  //
  // In short, just hold the lock during the entire operation!
  SharedMutex& getMutex(BucketId bid) const {
    return mutex_[bid.index() & (kNumMutexes - 1)];
  }

  folly::SpinLock& getBfLock(BucketId bid) const {
    return bfLock_[bid.index() & (kNumMutexes - 1)];
  }

  BucketId getBucketId(HashedKey hk) const {
    return BucketId{static_cast<uint32_t>(hk.keyHash() % numBuckets_)};
  }

  uint64_t getBucketOffset(BucketId bid) const {
    return cacheBaseOffset_ + bucketSize_ * bid.index();
  }

  double bfFalsePositivePct() const;
  void bfSet(BucketId bid, uint64_t bucket);
  void bfClear(BucketId bid);
  void bfRebuild(BucketId bid, const Bucket* bucket);
  bool bfReject(BucketId bid, uint64_t keyHash) const;

  // Use birthday paradox to estimate number of mutexes given number of parallel
  // queries and desired probability of lock collision.
  static constexpr size_t kNumMutexes = 16 * 1024;

  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 10;

  const ExpiredCheck checkExpired_{};
  const DestructorCallback destructorCb_{};
  const uint64_t bucketSize_{};
  const uint64_t cacheBaseOffset_{};
  const uint64_t numBuckets_{};
  std::unique_ptr<BloomFilter> bloomFilter_;
  std::chrono::nanoseconds generationTime_{};
  Device& device_;
  // handle for data placement technologies like FDP
  int placementHandle_;
  std::unique_ptr<SharedMutex[]> mutex_{new SharedMutex[kNumMutexes]};
  // Spinlocks for bloom filter operations
  // We use spinlock in addition to the mutex to avoid contentions of
  // couldExist which needs to be fast against other long running or
  // even blocking operations including insert/remove. When the race
  // happens against the remove or evict of the given item, there could
  // be a false positive which is ok.
  // Nested lock orders are always mutex-then-spinlock
  std::unique_ptr<folly::SpinLock[]> bfLock_{new folly::SpinLock[kNumMutexes]};

  // thread local counters in synchronized path
  mutable TLCounter lookupCount_;
  mutable TLCounter bfProbeCount_;
  mutable TLCounter bfRejectCount_;

  // atomic counters in asynchronized path
  mutable AtomicCounter itemCount_;
  mutable AtomicCounter insertCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionCount_;
  mutable AtomicCounter evictionExpiredCount_;
  mutable AtomicCounter logicalWrittenCount_;
  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter bfFalsePositiveCount_;
  mutable AtomicCounter bfRebuildCount_;
  mutable AtomicCounter checksumErrorCount_;
  mutable AtomicCounter usedSizeBytes_;
  // counters to quantify the expired eviction overhead (temporary)
  // PercentileStats generates outputs in integers, so amplify by 100x
  mutable util::PercentileStats bucketExpirationsDist_x100_;

  static_assert((kNumMutexes & (kNumMutexes - 1)) == 0,
                "number of mutexes must be power of two");
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/navy/block_cache/Index.h"

namespace facebook {
namespace cachelib {
namespace navy {
// for unit tests private members access
#ifdef FixedSizeIndex_TEST_FRIENDS_FORWARD_DECLARATION
FixedSizeIndex_TEST_FRIENDS_FORWARD_DECLARATION;
#endif

// TODO: Re-evaluate whether using fiber mutex is the right choice here. (Same
// with in the SparseMapIndex). When it's mostly memory operations, non-fiber
// mutex may be enough and using fiber mutex may introduce more overheads than
// benefit. Need to test and evaluate if non-fiber mutex is enough here.
//
// folly::SharedMutex is write priority by default
using SharedMutex =
    folly::fibers::TimedRWMutexWritePriority<folly::fibers::Baton>;

// NVM index implementation with the fixed size memory footprint.
// With the configured parameters given, it will decide how large the hash table
// is, and it won't rehash on run time.
//
// Unlike SparseMapIndex, FixedSizeIndex doesn't use or store fixed size sub-key
// (32bits in SparseMapIndex). In FixedSizeIndex, Total # of buckets (configured
// by # of chunks and # of buckets per chunk) will be used to decide the sub-key
// (chunk id + bucket id) size. It means that if the smaller number of total
// buckets is configured for FixedSizeIndex, it will increase the chances of
// hash collsion (false positive) which will be considered as the same key
// within the index. If it needs to be strictly managed, it's up to caller to
// set up proper configurtion numbers.
class FixedSizeIndex : public Index {
 public:
  explicit FixedSizeIndex(uint32_t numChunks,
                          uint8_t numBucketsPerChunkPower,
                          uint64_t numBucketsPerMutex)
      : numChunks_(numChunks),
        numBucketsPerChunkPower_(numBucketsPerChunkPower),
        numBucketsPerMutex_(numBucketsPerMutex) {
    initialize();
  }
  FixedSizeIndex() = delete;
  ~FixedSizeIndex() override = default;
  FixedSizeIndex(const FixedSizeIndex&) = delete;
  FixedSizeIndex(FixedSizeIndex&&) = delete;
  FixedSizeIndex& operator=(const FixedSizeIndex&) = delete;
  FixedSizeIndex& operator=(FixedSizeIndex&&) = delete;

  static constexpr double kSizeExpBase = 1.1925;

  // Writes index content to a Thrift object
  void persist(RecordWriter& rw) const override;

  // Resets index then inserts entries from a Thrift object read from
  // RecordReader.
  void recover(RecordReader& rr) override;

  // Gets value and update tracking counters
  LookupResult lookup(uint64_t key) override;

  // Gets value without updating tracking counters
  LookupResult peek(uint64_t key) const override;

  // Inserts an entry or overwrites existing entry with new address and size,
  // and it also will reset hits counting. If the entry was overwritten,
  // LookupResult.found() returns true and LookupResult.record() returns the old
  // record.
  // If given address is invalid (0 with PackedItemRecord::kInvalidAddress),
  // insert() won't succeed properly.
  LookupResult insert(uint64_t key,
                      uint32_t address,
                      uint16_t sizeHint) override;

  // Replaces old address with new address if there exists the key with the
  // identical old address. Current hits will be reset after successful replace.
  // All other fields in the record is retained.
  //
  // @return true if replaced.
  bool replaceIfMatch(uint64_t key,
                      uint32_t newAddress,
                      uint32_t oldAddress) override;

  // If the entry was successfully removed, LookupResult.found() returns true
  // and LookupResult.record() returns the record that was just found.
  // If the entry wasn't found, then LookupResult.found() returns false.
  LookupResult remove(uint64_t key) override;

  // Removes only if address match.
  //
  // @return true if removed successfully, false otherwise.
  bool removeIfMatch(uint64_t key, uint32_t address) override;

  // Resets all the buckets to the initial state.
  void reset() override;

  // Walks buckets and computes total index entry count
  size_t computeSize() const override;

  // Walks buckets and computes max/min memory footprint range that index will
  // currently use for the entries it currently has.
  MemFootprintRange computeMemFootprintRange() const override;

  // Exports index stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  // Internally, FixedSizeIndex will maintain each entry as PackedItemRecord
  // which is reduced size version of Index::ItemRecord, and there is missing
  // precision or info due to the smaller size, but those missing details are
  // not critical ones.
  struct FOLLY_PACK_ATTR PackedItemRecord {
    // encoded address
    uint32_t address{kInvalidAddress};
    // info about size and current hits.
    struct {
      uint8_t curHits : 2;
      uint8_t sizeExp : 6;
    } info{};

    // Instead of using 1-bit for a flag per item to say if it's a valid entry
    // or not, we will use the pre-defined invalid address value to decide the
    // validity. With the current block cache implementation, address 0 won't be
    // used for index address, so we are using it as the invalid address value
    // here.
    //
    // (Current BC implementation/design always stores the end of the slot
    // address (for the entry), so it will be always the address of the end of
    // entry descriptor. See BlockCache.h for more details)
    static constexpr uint32_t kInvalidAddress{0};

    PackedItemRecord() {}

    PackedItemRecord(uint32_t _address,
                     uint16_t _sizeHint,
                     uint8_t _currentHits)
        : address(_address) {
      info.curHits = truncateCurHits(_currentHits);
      info.sizeExp = sizeHintToExp(_sizeHint);
      XDCHECK(isValidAddress(_address));
    }

    static uint8_t sizeHintToExp(uint16_t sizeHint) {
      // Input value (sizeHint) is the unit of kMinAllocAlignSize
      // (i.e. sizeHint = 1 means 512Bytes currently).
      // We want to represent this 16bit value by exponent value with 6bits (a ^
      // 0) = 0, (a ^ 63) >= max value (65535), then we get a = 1.1925
      //, so we can represent sizeHint by the exponent of base 1.1925

      // TODO1: Will remove using exponents and multiplications and improve here
      // TODO2: Need to revisit and evaluate to see if we need the same
      // precision level for the larger sizes

      XDCHECK(sizeHint > 0) << sizeHint;
      constexpr double m = kSizeExpBase;
      double x = 1;
      int xp = 0;
      while (x < sizeHint) {
        x *= m;
        ++xp;
      }
      XDCHECK(xp < 64) << sizeHint << " " << xp;
      return xp;
    }

    static uint16_t sizeExpToHint(uint8_t sizeExp) {
      // TODO: Will remove using exponents and multiplications and improve here
      constexpr double m = kSizeExpBase;
      double sizeHint = 1;
      for (int j = 0; j < sizeExp; ++j) {
        sizeHint *= m;
      }
      return static_cast<uint16_t>(sizeHint);
    }

    static uint8_t truncateCurHits(uint8_t curHits) {
      return (curHits > 3) ? 3 : curHits;
    }

    static bool isValidAddress(uint32_t address) {
      return address != kInvalidAddress;
    }

    bool isValid() const { return isValidAddress(address); }
    uint16_t getSizeHint() const { return sizeExpToHint(info.sizeExp); }

    int bumpCurHits() {
      if (info.curHits < 3) {
        info.curHits++;
      }
      return info.curHits;
    }
  };
  static_assert(5 == sizeof(PackedItemRecord),
                "PackedItemRecord size is 5 bytes");

  void initialize();

  // Updates hits information of a key.
  void setHitsTestOnly(uint64_t key,
                       uint8_t currentHits,
                       uint8_t totalHits) override;

  uint64_t bucketId(uint64_t hash) const {
    uint64_t cid = (hash >> 32) % numChunks_;
    uint64_t bid = hash & (bucketsPerChunk_ - 1);

    return ((cid << numBucketsPerChunkPower_) + bid);
  }

  uint64_t mutexId(uint64_t bucketId) const {
    return bucketId / numBucketsPerMutex_;
  }

  // Configuration related variables
  const uint32_t numChunks_{0};
  const uint8_t numBucketsPerChunkPower_{0};
  const uint64_t numBucketsPerMutex_{0};

  uint64_t bucketsPerChunk_{0};
  uint64_t totalBuckets_{0};
  uint64_t totalMutexes_{0};

  std::unique_ptr<PackedItemRecord[]> ht_;
  std::unique_ptr<SharedMutex[]> mutex_;
  // The size for ht (stored bucket count) will be managed per Mutex basis
  std::unique_ptr<size_t[]> sizeForMutex_;

  // A helper class for exclusive locked access to a bucket.
  // It will lock the proper mutex with the given key when it's created.
  // recordRef() and sizeRef() will return the record and size with exclusively
  // locked bucket reference.
  // Locked mutex will be released when it's destroyed.
  class ExclusiveLockedBucket {
   public:
    explicit ExclusiveLockedBucket(uint64_t key, FixedSizeIndex& index)
        : bid_(index.bucketId(key)),
          mid_{index.mutexId(bid_)},
          lg_{index.mutex_[mid_]},
          record_{index.ht_[bid_]},
          size_{index.sizeForMutex_[mid_]} {}

    PackedItemRecord& recordRef() { return record_; }
    size_t& sizeRef() { return size_; }

   private:
    uint64_t bid_;
    uint64_t mid_;
    std::lock_guard<SharedMutex> lg_;
    PackedItemRecord& record_;
    size_t& size_;
  };

  // A helper class for shared locked access to a bucket.
  // It will lock the proper mutex with the given key when it's created.
  // recordRef() will return the record with shared locked bucket reference.
  // Locked mutex will be released when it's destroyed.
  class SharedLockedBucket {
   public:
    explicit SharedLockedBucket(uint64_t key, const FixedSizeIndex& index)
        : bid_(index.bucketId(key)),
          mid_{index.mutexId(bid_)},
          lg_{index.mutex_[mid_]},
          record_{index.ht_[bid_]} {}

    const PackedItemRecord& recordRef() const { return record_; }

   private:
    uint64_t bid_;
    uint64_t mid_;
    std::shared_lock<SharedMutex> lg_;
    const PackedItemRecord& record_;
  };

// For unit tests private member access
#ifdef FixedSizeIndex_TEST_FRIENDS
  FixedSizeIndex_TEST_FRIENDS;
#endif
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

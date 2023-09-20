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

#include <folly/Portability.h>
#include <folly/SharedMutex.h>
#include <folly/stats/QuantileEstimator.h>
#include <tsl/sparse_map.h>

#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <utility>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
// NVM index: map from key to value. Under the hood, stores key hash to value
// map. If collision happened, returns undefined value (last inserted actually,
// but we do not want people to rely on that).
class Index {
 public:
  // Specify 1 second window size for quantile estimator.
  static constexpr std::chrono::seconds kQuantileWindowSize{1};

  Index() = default;
  Index(const Index&) = delete;
  Index& operator=(const Index&) = delete;

  // Writes index to a Thrift object one bucket at a time and passes each bucket
  // to @persistCb. The reason for this is because the index can be very large
  // and serializing everything at once uses a lot of RAM.
  void persist(RecordWriter& rw) const;

  // Resets index then inserts entries read from @deserializer. Throws
  // std::exception on failure.
  void recover(RecordReader& rr);

  struct FOLLY_PACK_ATTR ItemRecord {
    // encoded address
    uint32_t address{0};
    // a hint for the item size. It is the responsibility of the caller
    // to translate this hint back into a usable size.
    uint16_t sizeHint{0};
    // total hits during this item's entire lifetime in cache
    uint8_t totalHits{0};
    // hits during the current window for this item (e.g. before re-admission)
    uint8_t currentHits{0};

    ItemRecord(uint32_t _address = 0,
               uint16_t _sizeHint = 0,
               uint8_t _totalHits = 0,
               uint8_t _currentHits = 0)
        : address(_address),
          sizeHint(_sizeHint),
          totalHits(_totalHits),
          currentHits(_currentHits) {}
  };
  static_assert(8 == sizeof(ItemRecord), "ItemRecord size is 8 bytes");

  struct LookupResult {
    friend class Index;

    bool found() const { return found_; }

    ItemRecord record() const {
      XDCHECK(found_);
      return record_;
    }

    uint32_t address() const {
      XDCHECK(found_);
      return record_.address;
    }

    uint16_t sizeHint() const {
      XDCHECK(found_);
      return record_.sizeHint;
    }

    uint8_t currentHits() const {
      XDCHECK(found_);
      return record_.currentHits;
    }

    uint8_t totalHits() const {
      XDCHECK(found_);
      return record_.totalHits;
    }

   private:
    ItemRecord record_;
    bool found_{false};
  };

  // Gets value and update tracking counters
  LookupResult lookup(uint64_t key);

  // Gets value without updating tracking counters
  LookupResult peek(uint64_t key) const;

  // Overwrites existing key if exists with new address and size, and it also
  // will reset hits counting. If the entry was successfully overwritten,
  // LookupResult.found() returns true and LookupResult.record() returns the old
  // record.
  LookupResult insert(uint64_t key, uint32_t address, uint16_t sizeHint);

  // Replaces old address with new address if there exists the key with the
  // identical old address. Current hits will be reset after successful replace.
  // All other fields in the record is retained.
  //
  // @return true if replaced.
  bool replaceIfMatch(uint64_t key, uint32_t newAddress, uint32_t oldAddress);

  // If the entry was successfully removed, LookupResult.found() returns true
  // and LookupResult.record() returns the record that was just found.
  // If the entry wasn't found, then LookupResult.found() returns false.
  LookupResult remove(uint64_t key);

  // Removes only if both key and address match.
  //
  // @return true if removed successfully, false otherwise.
  bool removeIfMatch(uint64_t key, uint32_t address);

  // Updates hits information of a key.
  void setHits(uint64_t key, uint8_t currentHits, uint8_t totalHits);

  // Resets all the buckets to the initial state.
  void reset();

  // Walks buckets and computes total index entry count
  size_t computeSize() const;

  // Exports index stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const;

 private:
  static constexpr uint32_t kNumBuckets{64 * 1024};
  static constexpr uint32_t kNumMutexes{1024};

  using Map = tsl::sparse_map<uint32_t, ItemRecord>;

  static uint32_t bucket(uint64_t hash) {
    return (hash >> 32) & (kNumBuckets - 1);
  }

  static uint32_t subkey(uint64_t hash) { return hash & 0xffffffffu; }

  folly::SharedMutex& getMutexOfBucket(uint32_t bucket) const {
    XDCHECK(folly::isPowTwo(kNumMutexes));
    return mutex_[bucket & (kNumMutexes - 1)];
  }

  folly::SharedMutex& getMutex(uint64_t hash) const {
    auto b = bucket(hash);
    return getMutexOfBucket(b);
  }

  Map& getMap(uint64_t hash) const {
    auto b = bucket(hash);
    return buckets_[b];
  }

  void trackRemove(uint8_t totalHits);

  // Experiments with 64 byte alignment didn't show any throughput test
  // performance improvement.
  std::unique_ptr<folly::SharedMutex[]> mutex_{
      new folly::SharedMutex[kNumMutexes]};
  std::unique_ptr<Map[]> buckets_{new Map[kNumBuckets]};

  mutable util::PercentileStats hitsEstimator_{kQuantileWindowSize};
  mutable AtomicCounter unAccessedItems_;

  static_assert((kNumMutexes & (kNumMutexes - 1)) == 0,
                "number of mutexes must be power of two");
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

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
#include <folly/fibers/TimedMutex.h>
#include <folly/stats/QuantileEstimator.h>
#include <tsl/sparse_map.h>

#include <cassert>
#include <chrono>
#include <functional>
#include <memory>
#include <utility>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
// for unit tests private members access
#ifdef SparseMapIndex_TEST_FRIENDS_FORWARD_DECLARATION
SparseMapIndex_TEST_FRIENDS_FORWARD_DECLARATION;
#endif

// folly::SharedMutex is write priority by default
using SharedMutex =
    folly::fibers::TimedRWMutexWritePriority<folly::fibers::Baton>;

// NVM index implementation using sparse_map.
// If collision happened, returns undefined value (last inserted actually,
// but we do not want people to rely on that).
class SparseMapIndex : public Index {
 public:
  // ExtraField can be used to add extra information for the index record.
  // -kTotalHits: Keeps track of the total hits for a given key since the
  //   insertion in the index.
  // -kItemHitHistory: Keeps track of item's hits history for the last 8
  // reinserts.
  enum class ExtraField { kTotalHits = 0, kItemHitHistory };

  static constexpr uint32_t kDefaultNumBucketMaps{64 * 1024};
  static constexpr uint32_t kDefaultBucketMapsPerMutex{64};

  explicit SparseMapIndex(uint32_t numBucketMaps,
                          uint32_t numBucketMapsPerMutex)
      : numBucketMaps_(numBucketMaps),
        numBucketMapsPerMutex_(numBucketMapsPerMutex) {
    initialize();
  }
  explicit SparseMapIndex(uint32_t numBucketMaps,
                          uint32_t numBucketMapsPerMutex,
                          ExtraField extraField)
      : numBucketMaps_(numBucketMaps),
        numBucketMapsPerMutex_(numBucketMapsPerMutex),
        extraField_(extraField) {
    initialize();
  }
  SparseMapIndex()
      : SparseMapIndex(kDefaultNumBucketMaps, kDefaultBucketMapsPerMutex) {}
  ~SparseMapIndex() override = default;
  SparseMapIndex(const SparseMapIndex&) = delete;
  SparseMapIndex(SparseMapIndex&&) = delete;
  SparseMapIndex& operator=(const SparseMapIndex&) = delete;
  SparseMapIndex& operator=(SparseMapIndex&&) = delete;

  // Writes index to a Thrift object one bucket map at a time and passes each
  // bucket map to @persistCb. The reason for this is because the index can be
  // very large and serializing everything at once uses a lot of RAM.
  void persist(RecordWriter& rw) const override;

  // Resets index then inserts entries read from @deserializer. Throws
  // std::exception on failure.
  void recover(RecordReader& rr) override;

  // Gets value and update tracking counters
  LookupResult lookup(uint64_t key) override;

  // Gets value without updating tracking counters
  LookupResult peek(uint64_t key) const override;

  // Overwrites existing key if exists with new address and size, and it also
  // will reset hits counting. If the entry was successfully overwritten,
  // LookupResult.found() returns true and LookupResult.record() returns the old
  // record.
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

  // Removes only if both key and address match.
  //
  // @return true if removed successfully, false otherwise.
  bool removeIfMatch(uint64_t key, uint32_t address) override;

  // Resets all the buckets to the initial state.
  void reset() override;

  // Walks bucket maps and computes total index entry count
  size_t computeSize() const override;

  // Walks bucket maps and computes max/min memory footprint range that index
  // will currently use for the entries it currently has. (Since sparse_map is
  // difficult to get the internal status without modifying its implementaion
  // directly, this function will calculate max/min memory footprint range by
  // considering the current entry count and sparse_map's implementation)
  MemFootprintRange computeMemFootprintRange() const override;

  // Exports index stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  // Configuration related variables
  const uint32_t numBucketMaps_{64 * 1024};
  const uint32_t numBucketMapsPerMutex_{64};
  const ExtraField extraField_{ExtraField::kTotalHits};

  uint32_t totalMutexes_{1024};

  void initialize();

  // Updates hits information of a key.
  void setHitsTestOnly(uint64_t key,
                       uint8_t currentHits,
                       uint8_t totalHits) override;

  using Map = tsl::sparse_map<uint32_t, ItemRecord>;

  uint32_t bucketMap(uint64_t hash) const {
    return (hash >> 32) & (numBucketMaps_ - 1);
  }

  uint32_t subkey(uint64_t hash) const { return hash & 0xffffffffu; }

  SharedMutex& getMutexOfBucketMap(uint32_t bucketMap) const {
    XDCHECK(folly::isPowTwo(totalMutexes_));
    return mutex_[bucketMap & (totalMutexes_ - 1)];
  }

  SharedMutex& getMutex(uint64_t hash) const {
    auto b = bucketMap(hash);
    return getMutexOfBucketMap(b);
  }

  Map& getMap(uint64_t hash) const {
    auto b = bucketMap(hash);
    return bucketMaps_[b];
  }

  void trackRemove(const ItemRecord&);

  // Experiments with 64 byte alignment didn't show any throughput test
  // performance improvement.
  std::unique_ptr<SharedMutex[]> mutex_;
  std::unique_ptr<Map[]> bucketMaps_;

  mutable util::PercentileStats hitsEstimator_{kQuantileWindowSize};
  mutable AtomicCounter unAccessedItems_;

// For unit tests private member access
#ifdef SparseMapIndex_TEST_FRIENDS
  SparseMapIndex_TEST_FRIENDS;
#endif
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

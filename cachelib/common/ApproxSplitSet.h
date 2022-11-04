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
#include <glog/logging.h>

#include <atomic>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <numeric>

#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {

// Set that can drop entries. It does that to maintain internal hash table's
// load factor and max collision chain length under control.
//
// Implementation uses Robin Hood hashing with limit on probes. Because of this
// we also allocate "probe limit" extra slots to avoid dealing with wrapping
// probing around.
template <typename UIntT>
class DropSet {
 public:
  using UInt = UIntT;

  explicit DropSet(uint32_t probeLimit)
      : DropSet{probeLimit, kInitialCapacity} {}
  DropSet(const DropSet&) = delete;
  DropSet& operator=(const DropSet&) = delete;

  DropSet(DropSet&& other) noexcept
      : probeLimit_{other.probeLimit_},
        capacity_{other.capacity_},
        size_{other.size_},
        creationTimeSecs_{other.creationTimeSecs_},
        table_{std::move(other.table_)} {}

  DropSet& operator=(DropSet&& other) noexcept {
    table_.reset();
    new (this) DropSet{std::move(other)};
    return *this;
  }

  void insert(UInt key) {
    // Rebuild at 90% full with allocation of +50% of space means minimum
    // load factor is 60% and average is 75%.
    // Although this computation is on the hot path, compiler elides division
    // and benchmark shows that there is no difference performance penalty
    // compared to storing this number precomputed.
    if (size_ >= rehashSize()) {
      DropSet dst{probeLimit_, rehashCapacity()};
      for (uint32_t i = 0; i < capacity_; i++) {
        if (table_[i] != kEmptyKey) {
          dst.insertNoResize(table_[i]);
        }
      }
      *this = std::move(dst);
    }
    insertNoResize(key);
  }

  bool lookup(UInt key) const {
    uint32_t lookupProbeDistance{0};
    uint32_t i = bucketOf(key);
    // If probe length is really small, we may try avoiding the DIB condition
    // all together and just scan up to probe limit. This optimization may be
    // very workload dependent though.
    while (lookupProbeDistance < probeLimit_) {
      DCHECK_LT(i, capacity_ + probeLimit_);
      if (table_[i] == key) {
        return true;
      }

      // our lookup key is not in its ideal position. we relocate a key only if
      // its probe distance is smaller than probe distance of  the key being
      // inserted. compare if the existing key has a higher probe distance. If
      // the key at current position has a lower probe distance from its
      // original bucket position, there is no chance of the key we are looking
      // for being relocated beyond this position
      if (table_[i] == kEmptyKey || lookupProbeDistance > probeDistance(i)) {
        break;
      }

      lookupProbeDistance++;
      i++;
    }
    return false;
  }

  void reset() {
    size_ = 0;
    std::memset(table_.get(), 0, tableByteSize());
    creationTimeSecs_ = util::getCurrentTimeSec();
  }

  uint32_t size() const { return size_; }

  uint32_t getCreationTimeSecs() const { return creationTimeSecs_; }

 private:
  static constexpr uint32_t kInitialCapacity{4};
  static constexpr UInt kEmptyKey{0};

  explicit DropSet(uint32_t probeLimit, uint32_t capacity)
      : probeLimit_{probeLimit},
        capacity_{capacity},
        creationTimeSecs_{util::getCurrentTimeSec()},
        table_{std::make_unique<UInt[]>(capacity + probeLimit)} {}

  size_t tableByteSize() const {
    return sizeof(UInt) * (capacity_ + probeLimit_);
  }

  uint32_t rehashSize() const { return capacity_ * 9 / 10; }

  uint32_t rehashCapacity() const { return std::max(capacity_ * 3 / 2, 16u); }

  uint32_t bucketOf(UInt key) const {
    // See "A fast alternative to the modulo reduction" by D. Lemire for proof.
    // Method works for well distributes keys (i.e., good hash function).
    return (uint64_t{key} * capacity_) >> 32;
  }

  uint32_t probeDistance(uint32_t pos) const {
    DCHECK_GE(pos, bucketOf(table_[pos]));
    return pos - bucketOf(table_[pos]);
  }

  void insertNoResize(UInt key) {
    uint32_t insertProbeDistance{0};
    uint32_t i = bucketOf(key);
    while (insertProbeDistance < probeLimit_) {
      DCHECK_LT(i, capacity_ + probeLimit_);
      if (table_[i] == kEmptyKey) {
        table_[i] = key;
        size_++;
        return;
      }
      auto currProbeDistance = probeDistance(i);
      if (currProbeDistance <= insertProbeDistance) {
        std::swap(key, table_[i]);
        insertProbeDistance = currProbeDistance;
      }
      insertProbeDistance++;
      i++;
    }
  }

  const uint32_t probeLimit_;
  const uint32_t capacity_;
  uint32_t size_{};
  time_t creationTimeSecs_; // creation time in seconds since epoch
  std::unique_ptr<UInt[]> table_;
};

// Approximate set made of several splits which are populated in FIFO order
class ApproxSplitSet {
 public:
  // May want to use 64 bit if too many collisions. Good for use cases we have.
  using Split = DropSet<uint32_t>;

  ApproxSplitSet(uint64_t numEntries, uint32_t numSplits)
      : numSplits_{numSplits},
        maxSplitSize_{static_cast<uint32_t>(numEntries / numSplits)} {
    if (numEntries == 0) {
      throw std::invalid_argument("NumEntries for ApproxDropSplit is 0");
    }
    addNewSplit();
  }

  // Returns true if @key already existed in the set before insert
  bool insert(uint64_t key) {
    std::lock_guard<std::mutex> l(mutex_);
    DCHECK(!splits_.empty());
    // Important to lookup first, because entry may be in another split, not the
    // one we're currently inserting into.
    auto exists = lookupLocked(key);
    if (!exists) {
      if (splits_.back().size() >= maxSplitSize_) {
        addNewSplit();
      }
      splits_.back().insert(key);
    }
    return exists;
  }

  void reset() {
    std::lock_guard<std::mutex> lock{mutex_};
    splits_.clear();
    addNewSplit();
  }

  uint32_t numSplits() const { return numSplits_; }

  uint64_t maxSplitSize() const { return maxSplitSize_; }

  uint64_t numKeysTracked() const {
    std::lock_guard<std::mutex> lock{mutex_};
    return std::accumulate(
        splits_.begin(), splits_.end(), 0ULL,
        [](uint64_t a, const Split& s) { return a += s.size(); });
  }

  uint32_t trackingWindowDurationSecs() const {
    std::lock_guard<std::mutex> lock{mutex_};
    if (splits_.empty()) {
      return 0;
    }
    return util::getCurrentTimeSec() - splits_.front().getCreationTimeSecs();
  }

 private:
  static constexpr uint32_t kProbeLimit{10};

  bool lookupLocked(uint64_t key) const {
    for (const auto& s : splits_) {
      if (s.lookup(key)) {
        return true;
      }
    }
    return false;
  }

  void addNewSplit() {
    if (splits_.size() < numSplits_) {
      splits_.push_back(Split{kProbeLimit});
    } else {
      DCHECK(!splits_.empty());
      auto s = std::move(splits_.front());
      s.reset();
      splits_.pop_front();
      splits_.push_back(std::move(s));
    }
  }

  mutable std::mutex mutex_;
  const uint32_t numSplits_{};
  const uint32_t maxSplitSize_{};
  std::deque<Split> splits_;
};

} // namespace cachelib
} // namespace facebook

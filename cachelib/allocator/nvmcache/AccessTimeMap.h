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

#include <folly/container/F14Map.h>
#include <folly/lang/Align.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Utils.h"

namespace facebook::cachelib {

// Sharded concurrent map from key hash to last-access timestamp.
// Tracks the most recent DRAM access time for NVM-resident items so that
// BlockCache reinsertion and time-to-access (TTA) stats use fresher
// timestamps than the on-disk EntryDesc value.
//
// Written on DRAM eviction of NvmClean BlockCache items, read during
// BlockCache region reclaim and NVM-to-DRAM promotion, and cleaned up
// when an item leaves NVM. Only BlockCache items are tracked (BigHash
// items don't reinsert). Not updated on every DRAM access to avoid
// mutex overhead on the read path.
class AccessTimeMap {
 public:
  // maxSize: approximate upper bound on total entries across all shards.
  // Enforced per-shard as ceil(maxSize / numShards). 0 means unbounded.
  explicit AccessTimeMap(size_t numShards, size_t maxSize = 0)
      : shards_(numShards),
        maxEntriesPerShard_(computeMaxEntriesPerShard(numShards, maxSize)) {}

  // Store a timestamp for the given key hash.
  // If inserting a new key causes the shard to exceed capacity, an arbitrary
  // existing entry in the same shard is evicted to restore the limit.
  void set(uint64_t keyHash, uint32_t accessTimeSecs) {
    sets_.inc();
    auto& shard = getShard(keyHash);
    std::lock_guard l(shard.mutex_);
    auto [it, inserted] = shard.map_.insert_or_assign(keyHash, accessTimeSecs);
    if (inserted) {
      // Evict an arbitrary entry (not the one we just inserted). The victim
      // is not LRU/FIFO — F14 iteration order is unspecified. This is
      // acceptable because the map is a best-effort timestamp cache.
      if (maxEntriesPerShard_ > 0 && shard.map_.size() > maxEntriesPerShard_) {
        auto victim = shard.map_.begin();
        if (victim == it) {
          ++victim;
        }
        shard.map_.erase(victim);
        evictions_.inc();
      } else {
        size_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  }

  // Retrieve the timestamp for the given key hash without removing it.
  std::optional<uint32_t> get(uint64_t keyHash) const {
    gets_.inc();
    auto& shard = getShard(keyHash);
    std::lock_guard l(shard.mutex_);
    auto it = shard.map_.find(keyHash);
    if (it == shard.map_.end()) {
      misses_.inc();
      return std::nullopt;
    }
    hits_.inc();
    return it->second;
  }

  // Retrieve and remove the timestamp for the given key hash.
  // Returns std::nullopt if not found.
  std::optional<uint32_t> getAndRemove(uint64_t keyHash) {
    getAndRemoves_.inc();
    auto& shard = getShard(keyHash);
    std::lock_guard l(shard.mutex_);
    auto it = shard.map_.find(keyHash);
    if (it == shard.map_.end()) {
      misses_.inc();
      return std::nullopt;
    }
    auto val = it->second;
    shard.map_.erase(it);
    size_.fetch_sub(1, std::memory_order_relaxed);
    hits_.inc();
    return val;
  }

  // Remove the entry for the given key hash, if present.
  void remove(uint64_t keyHash) {
    removes_.inc();
    auto& shard = getShard(keyHash);
    std::lock_guard l(shard.mutex_);
    if (shard.map_.erase(keyHash)) {
      size_.fetch_sub(1, std::memory_order_relaxed);
    }
  }

  // Approximate total number of entries. O(1), no locking.
  // May be slightly stale under concurrent modifications due to relaxed
  // memory ordering, but avoids the contention of locking all shards.
  size_t size() const { return size_.load(std::memory_order_relaxed); }

  // Counters are read without a global barrier, so a snapshot may be
  // internally inconsistent (e.g. hits + misses > gets).
  void getCounters(const util::CounterVisitor& visitor) const {
    visitor("navy_atm_size", size_.load(std::memory_order_relaxed),
            util::CounterVisitor::CounterType::COUNT);
    visitor("navy_atm_sets", sets_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_gets", gets_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_get_and_removes", getAndRemoves_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_removes", removes_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_hits", hits_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_misses", misses_.get(),
            util::CounterVisitor::CounterType::RATE);
    visitor("navy_atm_evictions", evictions_.get(),
            util::CounterVisitor::CounterType::RATE);
  }

 private:
  // Each shard is aligned to a cache line boundary to prevent false sharing
  // between shards accessed by different threads.
  struct alignas(folly::hardware_destructive_interference_size) Shard {
    mutable std::mutex mutex_;
    folly::F14FastMap<uint64_t, uint32_t> map_;
  };

  static size_t computeMaxEntriesPerShard(size_t numShards, size_t maxSize) {
    if (numShards == 0) {
      throw std::invalid_argument("AccessTimeMap requires numShards > 0");
    }
    return maxSize > 0 ? (maxSize + numShards - 1) / numShards : 0;
  }

  Shard& getShard(uint64_t keyHash) {
    return shards_[keyHash % shards_.size()];
  }

  const Shard& getShard(uint64_t keyHash) const {
    return shards_[keyHash % shards_.size()];
  }

  std::vector<Shard> shards_;
  const size_t maxEntriesPerShard_{0};
  std::atomic<size_t> size_{0};

  mutable TLCounter sets_;
  mutable TLCounter gets_;
  mutable TLCounter getAndRemoves_;
  mutable TLCounter removes_;
  mutable TLCounter hits_;
  mutable TLCounter misses_;
  mutable TLCounter evictions_;
};

} // namespace facebook::cachelib

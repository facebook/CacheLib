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

#include <folly/ScopeGuard.h>
#include <folly/container/F14Map.h>
#include <folly/lang/Align.h>
#include <folly/logging/xlog.h>

#include <atomic>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Time.h"
#include "cachelib/common/Utils.h"
#include "cachelib/navy/serialization/gen-cpp2/objects_types.h"
#include "cachelib/shm/ShmManager.h"

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
  static constexpr std::string_view kShmInfoName = "shm_atm_info";
  static constexpr std::string_view kShmDataName = "shm_atm_data";
  static constexpr uint32_t kVersion = 1;

  // Flat entry layout for serialization into shared memory.
  struct ShmEntry {
    uint64_t keyHash;
    uint32_t accessTimeSecs;
  };

  // maxSize: approximate upper bound on total entries across all shards.
  // Enforced per-shard as ceil(maxSize / numShards). 0 means unbounded.
  explicit AccessTimeMap(size_t numShards, size_t maxSize = 0)
      : numShards_(numShards),
        maxSize_(maxSize),
        shards_(numShards),
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

  void persist(ShmManager& shm) {
    auto startTimeSecs = util::getCurrentTimeSec();

    size_t entryCount = 0;
    for (auto& shard : shards_) {
      std::lock_guard l(shard.mutex_);
      entryCount += shard.map_.size();
    }

    // Remove any stale segments to avoid recovering old state on warm roll.
    shm.removeShm(std::string(kShmDataName));
    shm.removeShm(std::string(kShmInfoName));

    if (entryCount == 0) {
      XLOG(INFO) << "AccessTimeMap is empty, nothing to persist";
      return;
    }

    const size_t dataSize = entryCount * sizeof(ShmEntry);
    auto dataAddr = shm.createShm(std::string(kShmDataName), dataSize);
    auto* entries = reinterpret_cast<ShmEntry*>(dataAddr.addr);

    size_t idx = 0;
    for (auto& shard : shards_) {
      std::lock_guard l(shard.mutex_);
      for (const auto& [keyHash, accessTimeSecs] : shard.map_) {
        if (idx >= entryCount) {
          break;
        }
        entries[idx++] = ShmEntry{keyHash, accessTimeSecs};
      }
      if (idx >= entryCount) {
        break;
      }
    }

    navy::serialization::AccessTimeMapConfig cfg;
    *cfg.version() = static_cast<int32_t>(kVersion);
    *cfg.numShards() = static_cast<int64_t>(numShards_);
    *cfg.maxSize() = static_cast<int64_t>(maxSize_);
    *cfg.entryCount() = static_cast<int64_t>(idx);

    auto ioBuf = Serializer::serializeToIOBuf(cfg);
    auto infoAddr = shm.createShm(std::string(kShmInfoName), ioBuf->length());
    Serializer serializer(
        reinterpret_cast<uint8_t*>(infoAddr.addr),
        reinterpret_cast<uint8_t*>(infoAddr.addr) + ioBuf->length());
    serializer.writeToBuffer(std::move(ioBuf));

    XLOGF(INFO,
          "Persisted AccessTimeMap: {} entries, {} bytes to shared memory "
          "in {} secs",
          idx, dataSize, util::getCurrentTimeSec() - startTimeSecs);
  }

  // Restore entries from shared memory. On failure, starts empty.
  void recover(ShmManager& shm) {
    auto startTimeSecs = util::getCurrentTimeSec();

    ShmAddr infoAddr;
    try {
      infoAddr = shm.attachShm(std::string(kShmInfoName));
    } catch (const std::exception&) {
      XLOG(INFO) << "No AccessTimeMap shm segments found, starting empty";
      return;
    }

    SCOPE_EXIT {
      shm.removeShm(std::string(kShmInfoName));
      shm.removeShm(std::string(kShmDataName));
    };

    Deserializer deserializer(
        reinterpret_cast<uint8_t*>(infoAddr.addr),
        reinterpret_cast<uint8_t*>(infoAddr.addr) + infoAddr.size);
    auto cfg =
        deserializer.deserialize<navy::serialization::AccessTimeMapConfig>();

    if (static_cast<uint32_t>(*cfg.version()) != kVersion) {
      XLOGF(WARN,
            "AccessTimeMap version mismatch: stored={}, current={}. "
            "Starting empty.",
            *cfg.version(), kVersion);
      return;
    }

    const auto entryCount = static_cast<size_t>(*cfg.entryCount());
    if (entryCount == 0) {
      // persist() skips empty maps, so this may indicate corruption.
      XLOG(WARN) << "AccessTimeMap shm segment has zero entries but info "
                    "segment exists, possible corruption. Starting empty.";
      return;
    }

    ShmAddr dataAddr;
    try {
      dataAddr = shm.attachShm(std::string(kShmDataName));
    } catch (const std::exception& e) {
      XLOGF(WARN,
            "Failed to attach AccessTimeMap data segment: {}. "
            "Starting empty.",
            e.what());
      return;
    }

    const size_t expectedSize = entryCount * sizeof(ShmEntry);
    if (dataAddr.size < expectedSize) {
      XLOGF(WARN,
            "AccessTimeMap data segment size mismatch: got={}, expected={}. "
            "Starting empty.",
            dataAddr.size, expectedSize);
      return;
    }

    const auto* entries = reinterpret_cast<const ShmEntry*>(dataAddr.addr);

    // Group entries by shard so we only grab each lock once.
    std::vector<folly::F14FastMap<uint64_t, uint32_t>> perShard(shards_.size());
    for (size_t i = 0; i < entryCount; ++i) {
      auto shardIdx = entries[i].keyHash % shards_.size();
      perShard[shardIdx][entries[i].keyHash] = entries[i].accessTimeSecs;
    }
    size_t totalInserted = 0;
    for (size_t s = 0; s < shards_.size(); ++s) {
      if (perShard[s].empty()) {
        continue;
      }
      auto& shard = shards_[s];
      std::lock_guard l(shard.mutex_);
      size_t inserted = 0;
      for (const auto& [keyHash, ts] : perShard[s]) {
        if (maxEntriesPerShard_ > 0 &&
            shard.map_.size() >= maxEntriesPerShard_) {
          break;
        }
        shard.map_.insert_or_assign(keyHash, ts);
        ++inserted;
      }
      size_.fetch_add(inserted, std::memory_order_relaxed);
      totalInserted += inserted;
    }

    if (totalInserted < entryCount) {
      XLOGF(WARN,
            "AccessTimeMap recovery dropped {} entries due to per-shard "
            "size cap (maxEntriesPerShard={}, persisted={})",
            entryCount - totalInserted, maxEntriesPerShard_, entryCount);
    }

    XLOGF(INFO,
          "Recovered AccessTimeMap: {}/{} entries, {} bytes from shared memory "
          "in {} secs",
          totalInserted, entryCount, expectedSize,
          util::getCurrentTimeSec() - startTimeSecs);
  }

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

  const size_t numShards_;
  const size_t maxSize_;
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

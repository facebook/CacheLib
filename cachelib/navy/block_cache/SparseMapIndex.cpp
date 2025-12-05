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

#include "cachelib/navy/block_cache/SparseMapIndex.h"

#include <folly/Format.h>

#include "cachelib/navy/serialization/Serialization.h"

namespace facebook::cachelib::navy {

namespace {
// increase val if no overflow, otherwise do nothing
uint8_t safeInc(uint8_t val) {
  if (val < std::numeric_limits<uint8_t>::max()) {
    return val + 1;
  }
  return val;
}
} // namespace

void SparseMapIndex::initialize() {
  XDCHECK(numBucketMaps_ != 0 && numBucketMapsPerMutex_ != 0);
  XDCHECK(folly::isPowTwo(numBucketMaps_) &&
          folly::isPowTwo(numBucketMapsPerMutex_));
  XDCHECK(numBucketMaps_ >= numBucketMapsPerMutex_);

  totalMutexes_ = numBucketMaps_ / numBucketMapsPerMutex_;

  bucketMaps_ = std::make_unique<Map[]>(numBucketMaps_);
  mutex_ = std::make_unique<SharedMutexType[]>(totalMutexes_);

  XLOGF(
      INFO,
      "SparseMapIndex was created with numBucketMaps_ = {}, totalMutexes_ = {}",
      numBucketMaps_, totalMutexes_);
}

void SparseMapIndex::setHitsTestOnly(uint64_t key,
                                     uint8_t currentHits,
                                     uint8_t totalHits) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    it.value().currentHits = currentHits;
    it.value().extra.totalHits = totalHits;
  }
}

Index::LookupResult SparseMapIndex::lookup(uint64_t key) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    LookupResult lr{true, it->second};

    if (extraField_ == ExtraField::kTotalHits) {
      it.value().extra.totalHits = safeInc(it.value().extra.totalHits);
    }
    it.value().currentHits = safeInc(it.value().currentHits);
    return lr;
  }
  return {};
}

Index::LookupResult SparseMapIndex::peek(uint64_t key) const {
  const auto& map = getMap(key);
  auto lock = std::shared_lock{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    return LookupResult(true, it->second);
  }
  return {};
}

Index::LookupResult SparseMapIndex::insert(uint64_t key,
                                           uint32_t address,
                                           uint16_t sizeHint) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};
  auto it = map.find(subkey(key));
  if (it != map.end()) {
    LookupResult lr{true, it->second};

    trackRemove(it->second);
    // tsl::sparse_map's `it->second` is immutable, while it.value() is
    // mutable
    it.value().address = address;
    it.value().currentHits = 0;
    it.value().extra = {0};
    it.value().sizeHint = sizeHint;
    return lr;
  }
  map.try_emplace(subkey(key), address, sizeHint);

  return {};
}

bool SparseMapIndex::replaceIfMatch(uint64_t key,
                                    uint32_t newAddress,
                                    uint32_t oldAddress) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end() && it->second.address == oldAddress) {
    // tsl::sparse_map's `it->second` is immutable, while it.value() is
    // mutable
    it.value().address = newAddress;
    if (extraField_ == ExtraField::kItemHitHistory) {
      it.value().extra.itemHistory >>= 1;
      if (it.value().currentHits > 0) {
        it.value().extra.itemHistory |= 0x80;
      }
    }
    it.value().currentHits = 0;
    return true;
  }
  return false;
}

void SparseMapIndex::trackRemove(const ItemRecord& record) {
  int totalHits;

  switch (extraField_) {
  case ExtraField::kTotalHits:
    totalHits = record.extra.totalHits;
    break;
  case ExtraField::kItemHitHistory:
    // inaccurate, but unaccessed items will have 0 value and accessed won't
    auto estimatedTotalHits =
        std::max(__builtin_popcount(record.extra.itemHistory),
                 static_cast<int>(record.currentHits));
    totalHits = estimatedTotalHits;
    break;
  }

  hitsEstimator_.trackValue(totalHits);
  if (totalHits == 0) {
    unAccessedItems_.inc();
  }
}

Index::LookupResult SparseMapIndex::remove(uint64_t key) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end()) {
    LookupResult lr{true, it->second};

    trackRemove(it->second);
    map.erase(it);
    return lr;
  }
  return {};
}

bool SparseMapIndex::removeIfMatch(uint64_t key, uint32_t address) {
  auto& map = getMap(key);
  auto lock = std::lock_guard{getMutex(key)};

  auto it = map.find(subkey(key));
  if (it != map.end() && it->second.address == address) {
    trackRemove(it->second);
    map.erase(it);
    return true;
  }
  return false;
}

void SparseMapIndex::reset() {
  for (uint32_t i = 0; i < numBucketMaps_; i++) {
    auto lock = std::lock_guard{getMutexOfBucketMap(i)};
    bucketMaps_[i].clear();
  }
  unAccessedItems_.set(0);
}

size_t SparseMapIndex::computeSize() const {
  size_t size = 0;
  for (uint32_t i = 0; i < numBucketMaps_; i++) {
    auto lock = std::shared_lock{getMutexOfBucketMap(i)};
    size += bucketMaps_[i].size();
  }
  return size;
}

Index::MemFootprintRange SparseMapIndex::computeMemFootprintRange() const {
  // For now, this function's implementation is tightly coupled with the
  // sparse_map implementation that we curretly use and that is intended.
  // TODO: Make this function more general to cover other index implementation
  // by looking at the config value (when other index implementation is added)
  using sparseArray = tsl::detail_sparse_hash::sparse_array<
      std::pair<typename Map::key_type, typename Map::value_type>,
      typename Map::allocator_type, tsl::sh::sparsity::medium>;

  Index::MemFootprintRange range;
  auto sparseBucketSize = sizeof(sparseArray);
  // sparse_map is using std::pair<Key, Value> for actual data to be stored,
  // so actual size that is stored can be larger than sizeof(Key) +
  // sizeof(Value) due to the alignment within std::pair
  auto entrySize =
      sizeof(std::pair<typename Map::key_type, typename Map::value_type>);

  for (uint32_t i = 0; i < numBucketMaps_; i++) {
    auto lock = std::shared_lock{getMutexOfBucketMap(i)};

    // add the size of fixed mem used for sparse_map's member (sparse_hash)
    size_t bucketMapMemUsed = sizeof(bucketMaps_[i]);

    // The number of buckets is a power of 2 and one sparse array instance
    // will cover 64 buckets, so there will be (# of buckets) / 64 sparse
    // array instances (called sparse bucket in sparse_map implementation).
    auto sparseBucketCount =
        (bucketMaps_[i].bucket_count() == 0)
            ? 0
            : ((bucketMaps_[i].bucket_count() - 1) >> 6) + 1;
    bucketMapMemUsed += sparseBucketCount * sparseBucketSize;

    // For each entry in the hash table (sparse_hash)
    auto entryCount = bucketMaps_[i].size();

    // For memory consumed for the entries in the sparse_hash, we can only
    // calculate the range that it will consume without directly touching
    // sparse_map implementation. Real memory consumed by it is up to how hash
    // values of keys are distributed
    //
    // The default config for sparse_array will increase the real array by 4
    // per every resize. For the worst case, we may have 4 additional entries
    // per each sparse array instance (sparse bucket)
    // (# of real buckets)
    // = (# of entries - (# of entries mod 4)) + (# of sparse buckets) * 4
    range.maxUsedBytes +=
        bucketMapMemUsed +
        ((entryCount == 0)
             ? 0
             : ((((entryCount - 1) >> 2) << 2) + (sparseBucketCount << 2)) *
                   entrySize);

    // For the best case, all sparse_array will be filled with the exact
    // number (mod 4 == 0) of real buckets and only one sparse array may have
    // additional buckets
    // (# of real buckets)
    // = (# of entries - (# of entries mod 4)) + (1 or 0) * 4
    range.minUsedBytes +=
        bucketMapMemUsed + ((entryCount == 0)
                                ? 0
                                : ((((entryCount - 1) >> 2) << 2) +
                                   (((sparseBucketCount > 0) ? 1 : 0) << 2)) *
                                      entrySize);
  }
  return range;
}

void SparseMapIndex::persist(
    std::optional<std::reference_wrapper<RecordWriter>> rw) const {
  XDCHECK(rw.has_value());
  if (!rw) {
    XLOG(ERR,
         "Cannot persist Block Cache index: No RecordWriter is available.");
    return;
  }

  serialization::IndexBucket bucketMap;
  auto prevPos = rw->get().getCurPos();
  uint64_t persisted = 0;

  for (uint32_t i = 0; i < numBucketMaps_; i++) {
    *bucketMap.bucketId() = i;
    // Convert index entries to thrift objects
    for (const auto& [key, record] : bucketMaps_[i]) {
      serialization::IndexEntry entry;
      entry.key() = key;
      entry.address() = record.address;
      entry.sizeHint() = record.sizeHint;
      entry.totalHits() = record.extra.totalHits;
      entry.currentHits() = record.currentHits;
      bucketMap.entries()->push_back(entry);
    }

    try {
      // Serialize bucket and this may throw exception when it's exceeding the
      // meta data size limit
      serializeProto(bucketMap, rw->get());
      persisted += bucketMap.entries()->size();
    } catch (const std::exception& e) {
      // Log the error and more info on current index
      XLOGF(ERR,
            "Error persisting Block Cache Index: {}, persist() began at pos {} "
            "and current pos {}, trying to add {} entries from bucketMap {}",
            e.what(), prevPos, rw->get().getCurPos(),
            bucketMap.entries()->size(), i);
      auto memFootprint = computeMemFootprintRange();
      XLOGF(ERR,
            "Current Block cache items count: {}, persisted {} items, index "
            "size in memory {} "
            "(max) - {} (min)",
            computeSize(),
            persisted,
            memFootprint.maxUsedBytes,
            memFootprint.minUsedBytes);
      // rethrow to the caller
      throw;
    }
    // Clear contents to reuse memory.
    bucketMap.entries()->clear();
  }
}

void SparseMapIndex::recover(
    std::optional<std::reference_wrapper<RecordReader>> rr) {
  // initialize before recover just in case it's called not in init phase
  if (computeSize() > 0) {
    reset();
  }

  XDCHECK(rr.has_value());
  if (!rr) {
    XLOG(ERR,
         "Cannot recover Block Cache index: No RecordReader is available.");
    return;
  }

  for (uint32_t i = 0; i < numBucketMaps_; i++) {
    auto bucketMap = deserializeProto<serialization::IndexBucket>(rr->get());
    uint32_t id = *bucketMap.bucketId();
    if (id >= numBucketMaps_) {
      throw std::invalid_argument{folly::sformat(
          "Invalid bucket map id. Max bucketMaps: {}, bucket map id: {}",
          numBucketMaps_,
          id)};
    }
    for (auto& entry : *bucketMap.entries()) {
      bucketMaps_[id].try_emplace(*entry.key(),
                                  *entry.address(),
                                  *entry.sizeHint(),
                                  *entry.totalHits(),
                                  *entry.currentHits());
    }
  }
}

void SparseMapIndex::getCounters(const CounterVisitor& visitor) const {
  hitsEstimator_.visitQuantileEstimator(visitor, "navy_bc_item_hits");
  visitor("navy_bc_item_removed_with_no_access", unAccessedItems_.get());
}
} // namespace facebook::cachelib::navy

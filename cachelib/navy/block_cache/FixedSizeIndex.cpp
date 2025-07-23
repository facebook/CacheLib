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

#include "cachelib/navy/block_cache/FixedSizeIndex.h"

#include <folly/logging/xlog.h>

#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {

void FixedSizeIndex::initialize() {
  XDCHECK(numChunks_ != 0 && numBucketsPerChunkPower_ != 0 &&
          numBucketsPerMutex_ != 0);

  XDCHECK(numBucketsPerChunkPower_ <= 63);
  bucketsPerChunk_ = (1ull << numBucketsPerChunkPower_);
  totalBuckets_ = numChunks_ * bucketsPerChunk_;

  XDCHECK(numBucketsPerMutex_ <= totalBuckets_);
  totalMutexes_ = (totalBuckets_ - 1) / numBucketsPerMutex_ + 1;

  ht_ = std::make_unique<PackedItemRecord[]>(totalBuckets_);
  mutex_ = std::make_unique<SharedMutex[]>(totalMutexes_);
  sizeForMutex_ = std::make_unique<size_t[]>(totalMutexes_);

  bucketDistInfo_.initialize(totalBuckets_);
}

Index::LookupResult FixedSizeIndex::lookup(uint64_t key) {
  // TODO 1: We are holding a exclusive lock here because we're updating hit
  // count info (currentHits here). Need to re-evaluate if there's benefit by
  // using shared lock and loosly managing hit count update. (Same with in
  // SparseMapIndex)
  //
  // TODO 2: couldExist() currently calls Index::lookup(). Need to re-evaluate
  // if we should use peek() instead so that it could avoid exclusive lock and
  // also not sure if bumping up hit count with couldExist() makes sense.
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.recordRef().isValid()) {
    return LookupResult(true,
                        ItemRecord(elb.recordRef().address,
                                   elb.recordRef().getSizeHint(),
                                   0, /* totalHits */
                                   elb.recordRef().bumpCurHits()));
  }

  return {};
}

Index::LookupResult FixedSizeIndex::peek(uint64_t key) const {
  SharedLockedBucket slb{key, *this};

  if (slb.recordRef().isValid()) {
    return LookupResult(true,
                        ItemRecord(slb.recordRef().address,
                                   slb.recordRef().getSizeHint(),
                                   0, /* totalHits */
                                   slb.recordRef().info.curHits));
  }

  return {};
}

Index::LookupResult FixedSizeIndex::insert(uint64_t key,
                                           uint32_t address,
                                           uint16_t sizeHint) {
  LookupResult lr;
  ExclusiveLockedBucket elb{key, *this, true};

  if (elb.recordRef().isValid()) {
    lr = LookupResult(true,
                      ItemRecord(elb.recordRef().address,
                                 elb.recordRef().getSizeHint(),
                                 0, /* totalHits */
                                 elb.recordRef().info.curHits));
  } else {
    ++elb.sizeRef();
  }
  // TODO: need to combine this two ops into one to make sure updateDistInfo()
  // part is not missed
  elb.recordRef() = PackedItemRecord{address, sizeHint, /* currentHits */ 0};
  elb.updateDistInfo(key, *this);

  return lr;
}

bool FixedSizeIndex::replaceIfMatch(uint64_t key,
                                    uint32_t newAddress,
                                    uint32_t oldAddress) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.recordRef().address == oldAddress) {
    elb.recordRef().address = newAddress;
    elb.recordRef().info.curHits = 0;
    return true;
  }
  return false;
}

Index::LookupResult FixedSizeIndex::remove(uint64_t key) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.recordRef().isValid()) {
    LookupResult lr{true, ItemRecord(elb.recordRef().address,
                                     elb.recordRef().getSizeHint(),
                                     0, /* totalHits */
                                     elb.recordRef().info.curHits)};

    XDCHECK(elb.sizeRef() > 0);
    --elb.sizeRef();
    elb.recordRef() = PackedItemRecord{};
    return lr;
  }

  elb.recordRef() = PackedItemRecord{};
  return {};
}

bool FixedSizeIndex::removeIfMatch(uint64_t key, uint32_t address) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.recordRef().address == address) {
    elb.recordRef() = PackedItemRecord{};

    XDCHECK(elb.sizeRef() > 0);
    --elb.sizeRef();

    return true;
  }
  return false;
}

void FixedSizeIndex::reset() {
  uint64_t bucketId = 0;
  for (uint32_t i = 0; i < totalMutexes_; i++) {
    auto lock = std::lock_guard{mutex_[i]};
    for (uint64_t j = 0; j < numBucketsPerMutex_; ++j) {
      ht_[bucketId++] = PackedItemRecord{};
    }
    sizeForMutex_[i] = 0;
  }
}

size_t FixedSizeIndex::computeSize() const {
  size_t size = 0;
  for (uint32_t i = 0; i < totalMutexes_; i++) {
    auto lock = std::shared_lock{mutex_[i]};
    size += sizeForMutex_[i];
  }

  return size;
}

Index::MemFootprintRange FixedSizeIndex::computeMemFootprintRange() const {
  Index::MemFootprintRange range;

  size_t memUsage = 0;
  memUsage += totalBuckets_ * sizeof(PackedItemRecord);
  memUsage += totalMutexes_ * sizeof(SharedMutex);
  memUsage += totalMutexes_ * sizeof(size_t);

  // for BucketDistInfo
  memUsage += bucketDistInfo_.getBucketDistInfoBufSize();

  range.maxUsedBytes = memUsage;
  range.minUsedBytes = memUsage;
  return range;
}

void FixedSizeIndex::persist(RecordWriter& rw) const {
  // TODO: need to revisit persist and recover
  // : We already know that current persist and recover are not efficient
  // and we don't handle well when we write more than pre-configured metadata
  // size by serializing too many items.
  // We will change to use shm for FixedSizeIndex for persist/recover soon.
  // For now, this code will follow the same logic with the exisiting
  // SparseMapIndex
  XLOGF(INFO, "Persisting BlockCache hashtable: {} buckets", totalBuckets_);

  auto fillMapBuf = folly::IOBuf::wrapBuffer(bucketDistInfo_.fillMap_.get(),
                                             bucketDistInfo_.fillMapBufSize_);
  auto partialBitsBuf = folly::IOBuf::wrapBuffer(
      bucketDistInfo_.partialBits_.get(), bucketDistInfo_.partialBitsBufSize_);

  rw.writeRecord(std::move(fillMapBuf));
  rw.writeRecord(std::move(partialBitsBuf));

  for (uint64_t i = 0; i < totalBuckets_; ++i) {
    serialization::IndexEntry entry;
    entry.key() = i;
    entry.address() = ht_[i].address;
    entry.sizeHint() = ht_[i].getSizeHint();
    entry.currentHits() = (uint8_t)(ht_[i].info.curHits);

    serializeProto(entry, rw);
  }
  XLOG(INFO) << "Finished persisting BlockCache hashtable";
}

void FixedSizeIndex::recover(RecordReader& rr) {
  // TODO need to revisit persist and recover. See the comment in persist().
  XLOGF(INFO, "Recovering BlockCache hashtable: {} buckets", totalBuckets_);

  auto fillMapBuf = rr.readRecord();
  auto partialBitsBuf = rr.readRecord();

  if (fillMapBuf->length() != bucketDistInfo_.fillMapBufSize_ ||
      partialBitsBuf->length() != bucketDistInfo_.partialBitsBufSize_) {
    XLOG(ERR) << "Failed to recover BlockCache index. BucketDistInfo format is "
                 "different";
    return;
  }

  memcpy(
      bucketDistInfo_.fillMap_.get(), fillMapBuf->data(), fillMapBuf->length());
  memcpy(bucketDistInfo_.partialBits_.get(),
         partialBitsBuf->data(),
         partialBitsBuf->length());

  for (uint64_t i = 0; i < totalBuckets_; ++i) {
    auto entry = deserializeProto<serialization::IndexEntry>(rr);
    if (static_cast<uint64_t>(*entry.key()) >= totalBuckets_) {
      continue;
    }

    if (PackedItemRecord::isValidAddress(*entry.address())) {
      // valid entry
      ht_[*entry.key()] =
          PackedItemRecord{static_cast<uint32_t>(*entry.address()),
                           static_cast<uint16_t>(*entry.sizeHint()),
                           static_cast<uint8_t>(*entry.currentHits())};
      ++sizeForMutex_[*entry.key() / numBucketsPerMutex_];
    } else {
      ht_[*entry.key()] = PackedItemRecord{};
    }
  }
  XLOG(INFO) << "Finished recovering BlockCache hashtable";
}

void FixedSizeIndex::getCounters(const CounterVisitor&) const {
  // TODO: nothing to add for now
  return;
}

void FixedSizeIndex::setHitsTestOnly(uint64_t key,
                                     uint8_t currentHits,
                                     uint8_t totalHits) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.recordRef().isValid()) {
    elb.recordRef().info.curHits =
        PackedItemRecord::truncateCurHits(currentHits);
    XLOGF(INFO,
          "setHitsTestOnly() for {}. totalHits {} was discarded in "
          "FixedSizeIndex",
          key,
          totalHits);
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

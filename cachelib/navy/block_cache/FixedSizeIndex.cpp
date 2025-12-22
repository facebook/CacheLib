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

  mutex_ = std::make_unique<SharedMutexType[]>(totalMutexes_);
  bucketDistInfo_.initialize(totalBuckets_);
}

size_t FixedSizeIndex::getRequiredPreallocSize() const {
  // Need to preallocate buffer for those info which needs to be persistent
  // 1. Main hash table with PackedItemRecord entries
  // 2. Table entry count information per each mutex boundary
  // (validBucketsPerMutex_)
  // 3. BucketDistInfo
  return sizeof(PackedItemRecord) * totalBuckets_ + // for ht_
         sizeof(size_t) * totalMutexes_ +           // for validBucketsPerMutex_
         bucketDistInfo_.getBucketDistInfoBufSize(); // for bucketDistInfo_
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

  if (elb.isValidRecord()) {
    return LookupResult(true,
                        ItemRecord(elb.recordPtr()->address,
                                   elb.recordPtr()->getSizeHint(),
                                   0, /* totalHits */
                                   elb.recordPtr()->bumpCurHits()));
  }

  return {};
}

Index::LookupResult FixedSizeIndex::peek(uint64_t key) const {
  SharedLockedBucket slb{key, *this};

  if (slb.isValidRecord()) {
    return LookupResult(true,
                        ItemRecord(slb.recordPtr()->address,
                                   slb.recordPtr()->getSizeHint(),
                                   0, /* totalHits */
                                   slb.recordPtr()->info.curHits));
  }

  return {};
}

Index::LookupResult FixedSizeIndex::insert(uint64_t key,
                                           uint32_t address,
                                           uint16_t sizeHint) {
  LookupResult lr;
  ExclusiveLockedBucket elb{key, *this, true};

  XDCHECK(elb.bucketExist());
  if (elb.isValidRecord()) {
    lr = LookupResult(true,
                      ItemRecord(elb.recordPtr()->address,
                                 elb.recordPtr()->getSizeHint(),
                                 0, /* totalHits */
                                 elb.recordPtr()->info.curHits));
  } else {
    ++elb.validBucketCntRef();
  }
  // TODO: need to combine this two ops into one to make sure updateDistInfo()
  // part is not missed
  *elb.recordPtr() = PackedItemRecord{address, sizeHint, /* currentHits */ 0};
  elb.updateDistInfo(key, *this);

  return lr;
}

Index::LookupResult FixedSizeIndex::insertIfNotExists(uint64_t key,
                                                      uint32_t address,
                                                      uint16_t sizeHint) {
  LookupResult lr;
  ExclusiveLockedBucket elb{key, *this, true};

  XDCHECK(elb.bucketExist());
  if (elb.isValidRecord()) {
    lr = LookupResult(true,
                      ItemRecord(elb.recordPtr()->address,
                                 elb.recordPtr()->getSizeHint(),
                                 0, /* totalHits */
                                 elb.recordPtr()->info.curHits));
    return lr;
  }

  ++elb.validBucketCntRef();
  // TODO: need to combine this two ops into one to make sure updateDistInfo()
  // part is not missed
  *elb.recordPtr() = PackedItemRecord{address, sizeHint, /* currentHits */ 0};
  elb.updateDistInfo(key, *this);

  return lr;
}

bool FixedSizeIndex::replaceIfMatch(uint64_t key,
                                    uint32_t newAddress,
                                    uint32_t oldAddress) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.isValidRecord() && elb.recordPtr()->address == oldAddress) {
    elb.recordPtr()->address = newAddress;
    elb.recordPtr()->info.curHits = 0;
    return true;
  }
  return false;
}

Index::LookupResult FixedSizeIndex::remove(uint64_t key) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.isValidRecord()) {
    LookupResult lr{true, ItemRecord(elb.recordPtr()->address,
                                     elb.recordPtr()->getSizeHint(),
                                     0, /* totalHits */
                                     elb.recordPtr()->info.curHits)};

    XDCHECK(elb.validBucketCntRef() > 0);
    --elb.validBucketCntRef();
    *elb.recordPtr() = PackedItemRecord{};
    return lr;
  }

  if (elb.bucketExist()) {
    *elb.recordPtr() = PackedItemRecord{};
  }
  return {};
}

bool FixedSizeIndex::removeIfMatch(uint64_t key, uint32_t address) {
  ExclusiveLockedBucket elb{key, *this, false};

  if (elb.isValidRecord() && elb.recordPtr()->address == address) {
    *elb.recordPtr() = PackedItemRecord{};

    XDCHECK(elb.validBucketCntRef() > 0);
    --elb.validBucketCntRef();

    return true;
  }
  return false;
}

void FixedSizeIndex::reset() {
  XLOGF(INFO,
        "Resetting BlockCache hashtable with FixedSizeIndex: {} buckets",
        totalBuckets_);
  // check if we don't have shm enabled
  void* baseAddr = shmManager_
                       ? shmManager_
                             ->createShm(getShmName(kShmIndexName),
                                         getRequiredPreallocSize())
                             .addr
                       : util::mmapAlignedZeroedMemory(
                             util::getPageSize(), getRequiredPreallocSize());
  initWithBaseAddr(reinterpret_cast<uint8_t*>(baseAddr));

  uint64_t bucketId = 0;
  for (uint32_t i = 0; i < totalMutexes_; i++) {
    auto lock = std::lock_guard{mutex_[i]};
    for (uint64_t j = 0; j < numBucketsPerMutex_; ++j) {
      ht_[bucketId++] = PackedItemRecord{};
    }
    validBucketsPerMutex_[i] = 0;
  }

  if (shmManager_) {
    // Let's store currently used config to shm
    serialization::FixedSizeIndexConfig cfg;
    *cfg.version() = kFixedSizeIndexVersion;
    *cfg.numChunks() = static_cast<int32_t>(numChunks_);
    *cfg.numBucketsPerChunkPower() = numBucketsPerChunkPower_;
    *cfg.numBucketsPerMutex() = static_cast<int64_t>(numBucketsPerMutex_);

    auto ioBuf = Serializer::serializeToIOBuf(cfg);

    auto shmAddr =
        shmManager_->createShm(getShmName(kShmIndexInfoName), ioBuf->length());
    Serializer serializer(
        reinterpret_cast<uint8_t*>(shmAddr.addr),
        reinterpret_cast<uint8_t*>(shmAddr.addr) + ioBuf->length());
    serializer.writeToBuffer(std::move(ioBuf));

    XLOGF(INFO,
          "Created BlockCache hashtable with FixedSizeIndex on shared memory: "
          "{} buckets",
          totalBuckets_);
  } else {
    XLOGF(INFO,
          "Created BlockCache hashtable with FixedSizeIndex, persistency is "
          "disabled: {} buckets",
          totalBuckets_);
  }
}

size_t FixedSizeIndex::computeSize() const {
  size_t size = 0;
  for (uint32_t i = 0; i < totalMutexes_; i++) {
    auto lock = std::shared_lock{mutex_[i]};
    size += validBucketsPerMutex_[i];
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

void FixedSizeIndex::persist(
    std::optional<std::reference_wrapper<RecordWriter>> /* rw */) const {
  // FixedSizeIndex supports persistency by using shm and at this point, we
  // don't have anything to store here. Leaving this only with the log message
  // to have consistent API call flow with SparseMapIndex
  XLOG(INFO) << "Finished persisting BlockCache hashtable";
}

void FixedSizeIndex::recover(
    std::optional<std::reference_wrapper<RecordReader>> /* rr */) {
  XLOGF(INFO,
        "Recovering BlockCache hashtable with FixedSizeIndex: {} buckets",
        totalBuckets_);
  // If recover() fails for whatever reason, it will throw exception. This
  // exception will be caught in BlockCache::recover() and it will proceed with
  // reset() with empty cache entries

  if (!shmManager_) {
    // Can't support persistency. Exception will be caught in
    // BlockCache::recover()
    throw std::runtime_error("Cannot recover FixedSizeIndex without shm");
  }

  // Check stored config first
  auto infoAddr = shmManager_->attachShm(getShmName(kShmIndexInfoName));
  Deserializer deserializer(
      reinterpret_cast<uint8_t*>(infoAddr.addr),
      reinterpret_cast<uint8_t*>(infoAddr.addr) + infoAddr.size);

  auto storedCfg =
      deserializer.deserialize<serialization::FixedSizeIndexConfig>();
  if (!checkStoredConfig(storedCfg)) {
    // Stored config values are different from current value. Exception will be
    // caught in BlockCache::recover()
    throw std::runtime_error(
        "Failed to recover FixedSizeIndex. Different config value(s)");
  }

  // Looks like we have stored FixedSizeIndex with the same config values
  auto indexBaseAddr = shmManager_->attachShm(getShmName(kShmIndexName));

  initWithBaseAddr(reinterpret_cast<uint8_t*>(indexBaseAddr.addr));

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

  if (elb.isValidRecord()) {
    elb.recordPtr()->info.curHits =
        PackedItemRecord::truncateCurHits(currentHits);
    XLOGF(INFO,
          "setHitsTestOnly() for {}. totalHits {} was discarded in "
          "FixedSizeIndex",
          key,
          totalHits);
  }
}

bool FixedSizeIndex::checkStoredConfig(
    const serialization::FixedSizeIndexConfig& stored) {
  return (static_cast<uint32_t>(*stored.version()) == kFixedSizeIndexVersion &&
          static_cast<uint32_t>(*stored.numChunks()) == numChunks_ &&
          static_cast<uint8_t>(*stored.numBucketsPerChunkPower()) ==
              numBucketsPerChunkPower_ &&
          static_cast<uint64_t>(*stored.numBucketsPerMutex()) ==
              numBucketsPerMutex_);
}

void FixedSizeIndex::initWithBaseAddr(uint8_t* addr) {
  // In Shm, it's stored in this order:
  // ht_, validBucketsPerMutex_, bucketDistInfo (fillMap_, partialBits_)
  ht_ = reinterpret_cast<PackedItemRecord*>(addr);
  addr += sizeof(PackedItemRecord) * totalBuckets_;

  validBucketsPerMutex_ = reinterpret_cast<size_t*>(addr);
  addr += sizeof(size_t) * totalMutexes_;

  bucketDistInfo_.initWithBaseAddr(addr);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

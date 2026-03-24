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
#include <folly/fibers/TimedMutex.h>

#include "cachelib/common/Profiled.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/navy/block_cache/CombinedEntryBlock.h"
#include "cachelib/navy/block_cache/CombinedEntryManager.h"
#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/serialization/gen-cpp2/objects_types.h"
#include "cachelib/shm/ShmManager.h"

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

// Callback to retrieve the key (64bit hashed key) from the given address info.
// How to manage the key and value info within the given address of the flash is
// not the scope that FixedSizeIndex should be involved with, so it'll just use
// the callback function given when it's created.
using RetrieveKeyCallback = std::function<std::optional<uint64_t>(uint32_t)>;

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
  FixedSizeIndex(uint32_t numChunks,
                 uint8_t numBucketsPerChunkPower,
                 uint64_t numBucketsPerShard,
                 ShmManager* shmManager,
                 const std::string& name,
                 bool handleOverflow = false,
                 CombinedEntryManager* combinedEntryMgr = nullptr,
                 RetrieveKeyCallback retrieveKeyCb = nullptr)
      : numChunks_{numChunks},
        numBucketsPerChunkPower_{numBucketsPerChunkPower},
        numBucketsPerShard_{numBucketsPerShard},
        shmManager_{shmManager},
        name_{name},
        handleOverflow_{handleOverflow},
        combinedEntryMgr_{combinedEntryMgr},
        retrieveKeyCb_{std::move(retrieveKeyCb)} {
    initialize();
  }

  // This constructor is mainly for testing. Don't use it for the real use case
  FixedSizeIndex(uint32_t numChunks,
                 uint8_t numBucketsPerChunkPower,
                 uint64_t numBucketsPerShard)
      : FixedSizeIndex(numChunks,
                       numBucketsPerChunkPower,
                       numBucketsPerShard,
                       nullptr,
                       "",
                       false,
                       nullptr,
                       nullptr) {
    reset();
  }

  FixedSizeIndex() = delete;
  ~FixedSizeIndex() override = default;
  FixedSizeIndex(const FixedSizeIndex&) = delete;
  FixedSizeIndex(FixedSizeIndex&&) = delete;
  FixedSizeIndex& operator=(const FixedSizeIndex&) = delete;
  FixedSizeIndex& operator=(FixedSizeIndex&&) = delete;

  // This needs to be increased with any major changes to FixedSizeIndex.
  // If persist() detects stored version being different with the current
  // version number, it will give up on recovering and begin with the empty
  // index.
  static constexpr uint32_t kFixedSizeIndexVersion = 1;

  // Shm names for FixedSizeIndex
  static constexpr std::string_view kShmIndexInfoName =
      "shm_fixed_size_index_info";
  static constexpr std::string_view kShmIndexName = "shm_fixed_size_index";

  // Simple helpers
  static uint64_t getTotalBucketCount(uint32_t numChunks,
                                      uint8_t numBucketsPerChunkPower);
  static uint64_t getTotalShardCount(uint64_t numBuckets,
                                     uint64_t numBucketsPerShard);

  // Writes index content to a Thrift object
  void persist(
      std::optional<std::reference_wrapper<RecordWriter>> rw) const override;

  // Resets index then inserts entries from a Thrift object read from
  // RecordReader.
  void recover(std::optional<std::reference_wrapper<RecordReader>> rr) override;

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
  // Same as above but fails if the key already exists
  LookupResult insertIfNotExists(uint64_t key,
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

  // For combined entry block only
  bool isCombinedEntryBucketLocked(uint64_t bid) const override {
    return ht_[bid].isCombinedEntry();
  }

  // Update the address for the combined entry bucket
  void updateCombinedEntryBucketLocked(uint64_t bid,
                                       uint32_t address) override {
    ht_[bid].address = address;
  }

 private:
  class BucketDistInfo {
    // 1. It's assumed that caller (FixedSizeIndex) will handle all the
    // parameters validity for each function. It'll be only XDCHECKed here.
    // 2. # of buckets per shard for FixedSizeIndex should be multiple of 8 so
    // that each byte in this info won't be shared across the buckets shard
    // boundary
    // 3. For now, it's 2-bit for fill info and 8-bit for partial key bits. All
    // those are hard coded
   public:
    BucketDistInfo() = default;

    void initialize(uint64_t numBuckets) {
      XDCHECK(numBuckets > 0);

      numBuckets_ = numBuckets;
      // Using 2 bits per each bucket for fillMap info
      fillMapBufSize_ = (numBuckets - 1) / 4 + 1;
      partialBitsBufSize_ = numBuckets;
    }

    void initWithBaseAddr(uint8_t* addr) {
      fillMap_ = addr;
      // Each fillMap buf is one byte and each bucket uses 2 bits from there
      addr += fillMapBufSize_;
      partialBits_ = addr;
    }

    void updateBucketFillInfo(uint64_t bucketId,
                              uint8_t slotOffset,
                              uint8_t partialKeys) {
      XDCHECK(bucketId < numBuckets_) << bucketId;
      XDCHECK(slotOffset <= 0x3) << slotOffset;

      setBucketFillInfo(bucketId, slotOffset);
      setPartialBits(bucketId, partialKeys);
    }

    uint8_t getBucketFillOffset(uint64_t bucketId) const {
      XDCHECK(bucketId < numBuckets_) << bucketId;
      return getBucketFillInfo(bucketId);
    }

    uint8_t getPartialKey(uint64_t bucketId) const {
      XDCHECK(bucketId < numBuckets_) << bucketId;

      return getPartialBits(bucketId);
    }

    uint64_t getBucketDistInfoBufSize() const {
      return fillMapBufSize_ + partialBitsBufSize_;
    }

   private:
    void setBucketFillInfo(uint64_t bid, uint8_t slotOffset) {
      uint64_t idx = bid / 4;
      uint8_t offset = (bid & 0x3) << 1;
      fillMap_[idx] =
          (fillMap_[idx] & ~(0x3 << offset)) | (slotOffset << offset);
    }

    uint8_t getBucketFillInfo(uint64_t bid) const {
      return (fillMap_[bid / 4] >> ((bid & 0x3) << 1)) & 0x3;
    }

    void setPartialBits(uint64_t bid, uint8_t bits) {
      partialBits_[bid] = bits;
    }
    uint8_t getPartialBits(uint64_t bid) const { return partialBits_[bid]; }

    // fillMap_ is a array of uint8_t
    uint8_t* fillMap_{};
    uint64_t fillMapBufSize_{0};
    // partialBits_ is a array of uint8_t
    uint8_t* partialBits_{};
    uint64_t partialBitsBufSize_{0};
    uint64_t numBuckets_{0};

    friend class FixedSizeIndex;
  };

  void initialize();

  // Random prime numbers for the distance for next bucket slot to use.
  static constexpr std::array<uint8_t, 4> kNextSlotOffset{0, 23, 61, 97};

  // This offset will be used to indicate there's no valid bucket slot matching
  // the given key
  static constexpr uint8_t kInvalidBucketSlotOffset = 0xff;
  // This bucket id will be used to indicate there's no valid bucket for the key
  static constexpr uint64_t kInvalidBucketId = 0xffffffffffffffff;

  uint8_t decideBucketSlotOffset(uint64_t bid, uint64_t key) {
    auto sid = shardId(bid);
    // To store all the possible slot's bucket ids for this bid
    std::array<uint64_t, kNextSlotOffset.size()> slotBids{};
    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      // Store it to avoid same calculation multiple times
      slotBids[i] = calcSlotBucketId(bid, i);
    }

    // check if we have the entry in the combined entry block
    if (handleOverflow_) {
      auto res = checkCombinedIndexEntry(slotBids, key);
      if (res != kInvalidBucketSlotOffset) {
        return res;
      }
    }

    // Check if there's already one matching
    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      // Make sure we don't go across the shard boundary
      XDCHECK(shardId(slotBids[i]) == sid)
          << bid << " " << i << " " << slotBids[i];

      // TODO: Make it more readable these if condition
      if (ht_[slotBids[i]].isValid() && !ht_[slotBids[i]].isCombinedEntry() &&
          bucketDistInfo_.getBucketFillOffset(slotBids[i]) == i &&
          partialKeyBits(key) == bucketDistInfo_.getPartialKey(slotBids[i])) {
        return i;
      }
    }

    // No match. Find the empty one
    auto emptySlot = findOrMakeEmptySlotByMoving(slotBids);
    if (emptySlot.has_value()) {
      return *emptySlot;
    }

    if (handleOverflow_) {
      // Handle overflowed bucket.
      // If there's any combined entry slot, we can just use it
      for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
        if (ht_[slotBids[i]].isCombinedEntry()) {
          return i;
        }
      }

      // There's no combined entry along the possible slots for this bid, so
      // let's create one. We will add the current index entry to the combined
      // entry block and store it at offset 0.
      auto curBid = slotBids[0];
      auto orgBid =
          originalBucketId(curBid, bucketDistInfo_.getBucketFillOffset(curBid));
      auto combinedBlk = createCombinedIndexBlock(curBid, orgBid, ht_[curBid]);
      if (combinedBlk) {
        // For now, it's just added to the combined entries map on the memory
        combinedEntries_[sid][curBid] = std::move(combinedBlk);
        ht_[curBid].setCombinedEntry();
      }
      // By returning offset 0, if createCombinedIndexBlock() couldn't create
      // it, current entry will be just discarded . If we have created it and
      // set it at curBid, current entry was already added to the combined blk
      return 0;
    } else {
      // Let's just replace the current one
      return 0;
    }
  }

  uint8_t checkBucketSlotOffset(uint64_t bid, uint64_t key) const {
    // To store all the possible slot's bucket ids for this bid
    std::array<uint64_t, kNextSlotOffset.size()> slotBids{};

    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      // Store it to avoid same calculation multiple times
      slotBids[i] = calcSlotBucketId(bid, i);
      // Make sure we don't go across the shard boundary
      XDCHECK(shardId(slotBids[i]) == shardId(bid))
          << bid << " " << i << " " << slotBids[i];
    }

    // check if we have the entry in the combined entry block
    if (handleOverflow_) {
      auto res = checkCombinedIndexEntry(slotBids, key);
      if (res != kInvalidBucketSlotOffset) {
        return res;
      }
    }

    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      if (ht_[slotBids[i]].isValid() && !ht_[slotBids[i]].isCombinedEntry() &&
          bucketDistInfo_.getBucketFillOffset(slotBids[i]) == i &&
          partialKeyBits(key) == bucketDistInfo_.getPartialKey(slotBids[i])) {
        return i;
      }
    }
    return kInvalidBucketSlotOffset;
  }

  std::optional<uint8_t> findOrMakeEmptySlotByMoving(
      const std::array<uint64_t, kNextSlotOffset.size()>& slotBids) {
    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      if (!ht_[slotBids[i]].isValid()) {
        return i;
      }

      // Don't ever move any combined entry. Combined index entry could have
      // other org bid's entries too, and if it's moved, it'll be misplaced.
      if (ht_[slotBids[i]].isCombinedEntry()) {
        continue;
      }

      // If it's occupied and not for the same key's bucket, let's see if we
      // can move it
      auto curOffset = bucketDistInfo_.getBucketFillOffset(slotBids[i]);
      if (curOffset != i) {
        // Get the original bid before applying the offset.
        auto orgBid = originalBucketId(slotBids[i], curOffset);
        // Check if any sub bucket is empty and we can move this there
        for (size_t sub = 0; sub < kNextSlotOffset.size(); sub++) {
          if (sub == curOffset) {
            continue;
          }
          auto checkBid = calcSlotBucketId(orgBid, sub);
          if (!ht_[checkBid].isValid()) {
            // Move current one to this bucket
            ht_[checkBid] = ht_[slotBids[i]];
            ht_[slotBids[i]] = {};
            bucketDistInfo_.updateBucketFillInfo(
                checkBid, sub, bucketDistInfo_.getPartialKey(slotBids[i]));
            // Moved, let's use this bucket.
            return i;
          }
        }
      }
    }
    return std::nullopt;
  }

  uint8_t checkCombinedIndexEntry(
      const std::array<uint64_t, kNextSlotOffset.size()>& slotBids,
      uint64_t key) const {
    auto sid = shardId(slotBids[0]);
    for (size_t i = 0; i < kNextSlotOffset.size(); i++) {
      if (ht_[slotBids[i]].isCombinedEntry()) {
        // check if this combined entry has the index entry for the given key
        auto it = combinedEntries_[sid].find(slotBids[i]);
        if (it != combinedEntries_[sid].end() &&
            it->second->peekIndexEntry(key)) {
          return i;
        }
      }
    }
    // couldn't find the given key from the combined entry
    return kInvalidBucketSlotOffset;
  }

  std::optional<PackedItemRecord> getCombinedIndexEntry(uint64_t sid,
                                                        uint64_t bid,
                                                        uint64_t key) const {
    // For now, combined entry will be in memory only
    auto it = combinedEntries_[sid].find(bid);
    XDCHECK_NE(it, combinedEntries_[sid].end());

    auto res = it->second->getIndexEntry(key);
    if (!res.hasError()) {
      return res.value();
    }
    return std::nullopt;
  }

  // It will add or update (if it's already there) an index entry for the given
  // key and record.
  bool insertCombinedIndexEntry(uint64_t sid,
                                uint64_t bid,
                                uint64_t key,
                                const PackedItemRecord& record) {
    // For now, combined entry will be in memory only
    auto it = combinedEntries_[sid].find(bid);
    XDCHECK_NE(it, combinedEntries_[sid].end());

    auto res = it->second->addIndexEntry(bid, key, record);
    if (res == CombinedEntryStatus::kUpdated ||
        res == CombinedEntryStatus::kOk) {
      return true;
    }
    // Currently, only failing case is when it's full
    XDCHECK(res == CombinedEntryStatus::kFull);
    XLOGF(ERR,
          "Combined entry block for bid {} is full and couldn't add the "
          "index entry for key {}",
          bid, key);

    // TODO: For now, we don't handle overflowed combined entry block case.
    // So it's just returning simple boolean here
    return false;
  }

  bool removeCombinedIndexEntry(uint64_t sid, uint64_t bid, uint64_t key) {
    // For now, combined entry will be in memory only
    auto it = combinedEntries_[sid].find(bid);
    XDCHECK_NE(it, combinedEntries_[sid].end());

    return (it->second->removeIndexEntry(key) == CombinedEntryStatus::kOk);
  }

  // This helper will get the proper bucket id and record entry
  // Return value : The pair of <Bucket id, pointer to the record>
  std::pair<uint64_t, PackedItemRecord*> getBucket(uint64_t orgBid,
                                                   uint8_t slotOffset) const {
    if (slotOffset != kInvalidBucketSlotOffset) {
      auto bid = calcSlotBucketId(orgBid, slotOffset);
      return std::make_pair(bid, &ht_[bid]);
    } else {
      // There's no bucket for the given key
      return std::make_pair(kInvalidBucketId, nullptr);
    }
  }

  // Create a combined index entry block with the initial entry record given.
  //
  // curBid : the bucket id that this entry is currently stored at.
  // orgBid : the bucket id simply calculated by bucketId() (before placing it
  //          to the slot (curBid) using partial bits and fill info)
  //
  std::unique_ptr<CombinedEntryBlock> createCombinedIndexBlock(
      uint64_t curBid, uint64_t orgBid, const PackedItemRecord& record) {
    // First, we need to get the key info with the current record.
    auto key = getKeyFromAddress(record.address, curBid, orgBid);

    if (!key) {
      // Failed to get the key from address. We'll just discard this entry
      XLOGF(ERR,
            "Failed to get the key from the address {} to create a combined "
            "index entry. This entry will be discarded (bid {})",
            record.address, curBid);
      return {};
    }

    std::unique_ptr<CombinedEntryBlock> combinedBlk =
        std::make_unique<CombinedEntryBlock>();

    // It's newly created and this must succeed
    auto status = combinedBlk->addIndexEntry(curBid, key.value(), record);
    if (status != CombinedEntryStatus::kOk) {
      // Something is wrong
      XLOGF(ERR,
            "Adding Key hash {}, bid {} to CombinedEntryBlock failed, "
            "status={}",
            key.value(), curBid, static_cast<uint8_t>(status));
      // We will continue by discarding currently stored entry
      return {};
    }
    return combinedBlk;
  }

  std::optional<uint64_t> getKeyFromAddress(uint32_t addr,
                                            uint64_t bid,
                                            uint64_t orgBid) {
    auto key = retrieveKeyCb_(addr);

    if (!key) {
      // TODO: Since we don't release the mutex for index while retrieving key
      // hash for now, there's no case for the race condition on index access
      // and modification. So, if we don't get the key hash, it's purely
      // because it can't read it properly. We can just continue and discard
      // currently stored entry.
      XLOGF(ERR,
            "Failed to retrieve the key from the address {} stored in bid = "
            "{}, org bid = {}",
            addr, bid, orgBid);
      return std::nullopt;
    }

    // check for the integrity (if retrieved key should be placed in this
    // bucket)
    XDCHECK(bucketId(key.value()) == orgBid &&
            partialKeyBits(key.value()) == bucketDistInfo_.getPartialKey(bid));

    return key;
  }

  // Updates hits information of a key.
  void setHitsTestOnly(uint64_t key,
                       uint8_t currentHits,
                       uint8_t totalHits) override;

  // These are random prime numbers for distributing bucket id
  static constexpr uint16_t kBucketRandomizeOffset[64] = {
      1229, 1543, 1867, 2179, 2521, 2833, 3203, 3539, 3877, 4229, 4567,
      4919, 5231, 5557, 5861, 6217, 1097, 1423, 1693, 2003, 3121, 3463,
      4099, 4451, 4793, 5119, 5483, 5827, 6197, 6547, 6899, 7283, 7649,
      5717, 6011, 7127, 7459, 7867, 8011, 8167, 8293, 8447, 8623, 8737,
      8863, 9013, 9173, 9319, 9437, 7103, 6823, 6959, 7079, 7993, 8147,
      8431, 8719, 9007, 9293, 9551, 9851, 2203, 4967, 5653};

  // Current bit mapping from the hash (64bits) to the bucket id and others
  // bit 0 ~ 31 : Reserved for bucket offset within chunk
  // bit 32 ~  : Used for chunk id modulo calculation. The numChunks can be
  //             configured as any integer up to 2^8 - 1 (8bit)
  // bit 40 ~ 47 : Used for partial key bits (For utilizing 4 bucket slots)
  // bit 48 ~ 61 : Used for randomizing bucket id
  uint64_t bucketId(uint64_t hash) const {
    uint64_t cid = (hash >> 32) % numChunks_;
    uint64_t bid = hash & (bucketsPerChunk_ - 1);

    // Random offset to further distribute bucket id by using the bits 48 ~ 61
    // from hashed key
    // offset will be decided by using 6bit + 6bit and 2bit as the weight
    uint64_t randomizeOffset1 = (hash >> 48) & 0x3F;
    uint64_t randomizeOffset2 = (hash >> 54) & 0x3F;
    uint64_t addOffset2 = (hash >> 60) & 0x3;

    // Adding calculated offset to bid to randomize distribution for the
    // bucket
    return (((cid << numBucketsPerChunkPower_) + bid) +
            kBucketRandomizeOffset[randomizeOffset1] +
            (kBucketRandomizeOffset[randomizeOffset2] << addOffset2)) %
           totalBuckets_;
  }

  // sharding is based on each mutex boundary
  uint64_t shardId(uint64_t bucketId) const {
    return bucketId / numBucketsPerShard_;
  }

  // Return the partial key bits to be used for hash collision open addressing
  uint8_t partialKeyBits(uint64_t key) const {
    // TODO: this is temporary and hard coded one... Need to think about more
    // where to choose
    return ((key >> 40) & 0xff);
  }

  // Get the next slot bucket id to check or use
  uint64_t calcSlotBucketId(uint64_t bid, uint8_t offset) const {
    // We don't want to go across the shard boundary, so if it goes beyond
    // that, it will wrap around and go back to the beginning of current shard
    // boundary
    XDCHECK(offset < kNextSlotOffset.size()) << offset;
    return (bid / numBucketsPerShard_) * numBucketsPerShard_ +
           ((bid + kNextSlotOffset[offset]) % numBucketsPerShard_);
  }

  // Some helper functions below
  //

  // Get the original bid before applying the slot offset. Also need to
  // consider the wraparound on the shard boundary.
  uint64_t originalBucketId(uint64_t curBid, uint8_t fillOffset) {
    return ((curBid % numBucketsPerShard_) >= kNextSlotOffset[fillOffset])
               ? curBid - kNextSlotOffset[fillOffset]
               : curBid + numBucketsPerShard_ - kNextSlotOffset[fillOffset];
  }

  bool checkStoredConfig(const serialization::FixedSizeIndexConfig& stored);
  size_t getRequiredPreallocSize() const;
  std::string getShmName(const std::string_view& namePrefix) {
    return std::string(namePrefix) + "_" + name_;
  }
  void initWithBaseAddr(uint8_t* addr);

  // Configuration related variables
  const uint32_t numChunks_{0};
  const uint8_t numBucketsPerChunkPower_{0};
  const uint64_t numBucketsPerShard_{0};
  ShmManager* shmManager_{};
  std::string name_;
  // TODO: This field is probably for temporary, until it's all evaluated and
  // validated to use flash for overflowed index entries
  const bool handleOverflow_{false};
  CombinedEntryManager* combinedEntryMgr_{};
  const RetrieveKeyCallback retrieveKeyCb_;

  uint64_t bucketsPerChunk_{0};
  uint64_t totalBuckets_{0};
  uint64_t totalShards_{0};

  // ht_ is a array of PackedItemRecord
  PackedItemRecord* ht_{};

  using SharedMutexType =
      trace::Profiled<SharedMutex, "cachelib:navy:bc_fixed_index">;
  std::unique_ptr<SharedMutexType[]> mutex_;

  // The size for ht (stored bucket count) will be managed per each shard
  // validBucketsPerShard_ is a array of size_t
  size_t* validBucketsPerShard_{};

  // Before we fully use flash for combined entries, this is an intermediate
  // step to maintain combined entries only on memory.
  std::unique_ptr<
      folly::F14FastMap<uint64_t, std::unique_ptr<CombinedEntryBlock>>[]>
      combinedEntries_;

  BucketDistInfo bucketDistInfo_;

  // A helper class for exclusive locked access to a bucket.
  // It will lock the mutex for the shard with the given key when it's created.
  // recordPtr() and validBucketCntRef() will return the record and
  // valid bucket count with exclusively locked bucket. Locked
  // mutex will be released when it's destroyed.
  class ExclusiveLockedBucket {
   public:
    explicit ExclusiveLockedBucket(uint64_t key,
                                   FixedSizeIndex& index,
                                   bool alloc)
        : bid_(index.bucketId(key)),
          sid_{index.shardId(bid_)},
          key_(key),
          lg_{index.mutex_[sid_]},
          validBuckets_{index.validBucketsPerShard_[sid_]} {
      auto offset = alloc ? index.decideBucketSlotOffset(bid_, key)
                          : index.checkBucketSlotOffset(bid_, key);
      std::tie(bid_, htEntry_) = index.getBucket(bid_, offset);
      slotOffset_ = offset;
      if (htEntry_) {
        if (!htEntry_->isCombinedEntry()) {
          record_ = *htEntry_;
        } else {
          auto res = index.getCombinedIndexEntry(sid_, bid_, key);
          if (res.has_value()) {
            record_ = res.value();
            // TODO: For now, hit count for combined index entry is only
            // updated to combined entry itself. So hit count should be
            // adjusted
            record_.bumpCurHits(htEntry_->info.curHits);
          }
        }
      }
    }

    // Returned reference should be always used for read only.
    const PackedItemRecord& recordRef() { return record_; }
    size_t& validBucketCntRef() { return validBuckets_; }
    void updateDistInfo(uint64_t key, FixedSizeIndex& index) {
      if (slotOffset_ != kInvalidBucketSlotOffset &&
          !htEntry_->isCombinedEntry()) {
        index.bucketDistInfo_.updateBucketFillInfo(bid_, slotOffset_,
                                                   index.partialKeyBits(key));
      }
    }

    // htEntry_ is pointing to the bucket entry, while record_ is the actual
    // record for the given key. record_ could be invalid even when we have
    // the htEntry_ located for the given key.
    bool isValidRecord() const { return (record_.isValid()); }

    // If there's no bucket which can possibly have the record for the given
    // key, htEntry_ is nullptr.
    bool bucketExist() const { return htEntry_ != nullptr; }

    void updateRecord(const PackedItemRecord newRec, FixedSizeIndex& index) {
      if (!htEntry_->isCombinedEntry()) {
        *htEntry_ = newRec;
        record_ = newRec;
      } else {
        if (index.insertCombinedIndexEntry(sid_, bid_, key_, newRec)) {
          record_ = newRec;
        } else {
          // Even when it fails, old one was removed within
          // insertCombinedIndexEntry()
          record_ = PackedItemRecord{};
        }
      }
    }

    bool updateNewAddress(uint32_t newAddress, FixedSizeIndex& index) {
      // This will be mainly used when the entry was moved to different
      // location and only the address needs to be updated (ex. with
      // reclaim-reinsert)
      //
      // We need a separate function from updateRecrod() since we don't keep
      // sizeExp for each record and we don't want to re-calculate for sizeHint
      // <-> sizeExp conversion
      if (!htEntry_->isCombinedEntry()) {
        htEntry_->address = newAddress;
        htEntry_->info.curHits = 0;
        record_ = *htEntry_;
      } else {
        record_.address = newAddress;
        record_.info.curHits = 0;
        if (!index.insertCombinedIndexEntry(sid_, bid_, key_, record_)) {
          // Even when it fails, old one was removed within
          // insertCombinedIndexEntry()
          record_ = PackedItemRecord{};
          return false;
        }
      }
      return true;
    }

    void removeRecord(FixedSizeIndex& index) {
      if (!htEntry_->isCombinedEntry()) {
        *htEntry_ = PackedItemRecord{};
      } else {
        index.removeCombinedIndexEntry(sid_, bid_, key_);
      }
      record_ = PackedItemRecord{};
    }

    int bumpCurRecordHits() {
      // TODO: Need to think about how to handle/manage hit count update for
      // combined entries. We can't update/write the entry everytime it's
      // looked up. For now, we will just use hitcount for the combined entry
      // block itself. Not precise, but still related value.
      return htEntry_->bumpCurHits();
    }

    // This function is only for test purpose.
    void setCurRecordHits(uint8_t newHits) {
      // Since it's only for testing, it assumes non combined entry
      XDCHECK(htEntry_ != nullptr && record_.isValid() &&
              !htEntry_->isCombinedEntry());
      htEntry_->info.curHits = newHits;
      record_.info.curHits = newHits;
    }

   private:
    uint64_t bid_;
    uint64_t sid_;
    uint64_t key_;
    std::lock_guard<SharedMutexType> lg_;
    size_t& validBuckets_;
    PackedItemRecord* htEntry_{};
    PackedItemRecord record_{};
    uint8_t slotOffset_{kInvalidBucketSlotOffset};
  };

  // A helper class for shared locked access to a bucket.
  // It will lock the proper mutex with the given key when it's created.
  // recordPtr() will return the record with shared locked bucket.
  // Locked mutex will be released when it's destroyed.
  class SharedLockedBucket {
   public:
    explicit SharedLockedBucket(uint64_t key, const FixedSizeIndex& index)
        : bid_(index.bucketId(key)),
          sid_{index.shardId(bid_)},
          lg_{index.mutex_[sid_]} {
      // check next bucket if it should be used
      std::tie(bid_, htEntry_) =
          index.getBucket(bid_, index.checkBucketSlotOffset(bid_, key));
      if (htEntry_) {
        if (!htEntry_->isCombinedEntry()) {
          record_ = *htEntry_;
        } else {
          auto res = index.getCombinedIndexEntry(sid_, bid_, key);
          if (res.has_value()) {
            record_ = res.value();
            // TODO: For now, hit count for combined index entry is only
            // updated to combined entry itself. So hit count should be
            // adjusted
            record_.bumpCurHits(htEntry_->info.curHits);
          }
        }
      }
    }

    const PackedItemRecord& recordRef() const { return record_; }
    bool isValidRecord() const { return (record_.isValid()); }

   private:
    uint64_t bid_;
    uint64_t sid_;
    std::shared_lock<SharedMutexType> lg_;
    const PackedItemRecord* htEntry_{};
    PackedItemRecord record_{};
  };

  friend class CombinedEntryBlock;
// For unit tests private member access
#ifdef FixedSizeIndex_TEST_FRIENDS
  FixedSizeIndex_TEST_FRIENDS;
#endif
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

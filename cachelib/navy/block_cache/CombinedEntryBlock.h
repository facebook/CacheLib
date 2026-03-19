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
#include <folly/container/F14Set.h>

#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

enum class CombinedEntryStatus : uint8_t {
  kOk = 0,
  kUpdated,
  kFull,
  kNotFound,
  kError,
};

using EntryPos = uint16_t;
using EntryIdx = uint16_t;
using EntryRecord = Index::PackedItemRecord;

// Combined entry block is a special block which contains multiple entries'
// info. It will be used to maintain overflowed index entries and also to have
// multiple small sized items.
// Since it could have variable sized items, for quick scan and lookup for each
// entry's info, its layout is divided into two parts. The basic info for each
// entry will be added from the front. Actual content for each entry will be
// added from the back and grow in reverse direction. The basic entry info will
// maintain the exact offset where the entry's content begins.
//
//  -------------------------------------------------------------------
//  | Entry Info  -->                            <--   Actual content |
//  -------------------------------------------------------------------
//
class CombinedEntryBlock {
 public:
  // Size for the combined entry block
  static constexpr uint16_t kDefaultSize = 4096;

  // For the purpose of CombinedEntryBlock, it doesn't need to be large size
  // hence blockSize is limited to 16bits
  CombinedEntryBlock(uint16_t blockSize = kDefaultSize)
      : allocated_{std::make_unique<uint8_t[]>(blockSize)},
        bufView_(blockSize, allocated_.get()),
        headerPtr_((Header*)bufView_.data()),
        // The first portion is for the header
        entryPosInfoBase_(bufView_.data() + sizeof(Header)),
        curPos_{blockSize} {
    initHeader();
  }

  CombinedEntryBlock(uint8_t* buf, uint16_t blockSize)
      : bufView_(blockSize, buf),
        headerPtr_((Header*)bufView_.data()),
        // The first portion is for the header
        entryPosInfoBase_(bufView_.data() + sizeof(Header)),
        curPos_{blockSize} {
    initHeader();
  }

  // Adding a index entry as the content of Combined entry block
  CombinedEntryStatus addIndexEntry(uint64_t bid,
                                    uint64_t key,
                                    const EntryRecord& record);

  // Get the index entry for the given key
  folly::Expected<EntryRecord, CombinedEntryStatus> getIndexEntry(uint64_t key);

  // Remove the index entry for the given key
  CombinedEntryStatus removeIndexEntry(uint64_t key);

  // Peek if we have a valid index entry for the given key
  bool peekIndexEntry(uint64_t key);

  // Clear all the contents of the combined entry block
  void clear();

  // Get the number of stored entries
  uint32_t numStoredEntries() const { return headerPtr_->numStoredEntries; }
  // Get the number of valid entries
  uint16_t numValidEntries() const { return numValidEntries_; }

  uint16_t getSize() const { return static_cast<uint16_t>(bufView_.size()); }
  uint16_t getUsableSize() const {
    return (uint16_t)(bufView_.size() - sizeof(Header));
  }

  struct EntryPosInfo {
    // TODO: There will be padding here, but we'll probably need additional
    // fields in this struct anyway
    uint64_t bid{};
    uint64_t key{};
    EntryPos pos{};
    struct {
      uint8_t removed : 1;
      uint8_t reserved : 7;
    } flag{};
  };

 private:
  EntryPos getEntryPos(uint16_t keyIdx) const {
    XDCHECK(sizeof(Header) + (keyIdx + 1) * sizeof(EntryPosInfo) <= curPos_);
    return (reinterpret_cast<const EntryPosInfo*>(entryPosInfoBase_)[keyIdx])
        .pos;
  }

  EntryPos getEmptySpacePos(uint16_t numEntry) const {
    return sizeof(Header) + sizeof(EntryPosInfo) * numEntry;
  }

  EntryPosInfo& entryPosInfoRef(uint16_t keyIdx) {
    return reinterpret_cast<EntryPosInfo*>(entryPosInfoBase_)[keyIdx];
  }

  // EntryRecord pos is the offset within the entire buffer including Header
  EntryRecord& entryRecordRef(uint16_t pos) {
    return *reinterpret_cast<EntryRecord*>(bufView_.data() + pos);
  }

  void increaseKeysForBid(uint64_t bid) { keysForBids_[bid]++; }

  void decreaseKeysForBid(uint64_t bid) {
    XDCHECK_GT(keysForBids_[bid], 0);
    keysForBids_[bid]--;
    if (keysForBids_[bid] == 0) {
      keysForBids_.erase(bid);
    }
  }

  void initHeader() {
    ::memcpy(headerPtr_->sig, kCombinedEntryHeaderSig, kCebHeaderSigLen);
    headerPtr_->numStoredEntries = 0;
  }

  static constexpr const char* kCombinedEntryHeaderSig = "CEBENTRY";
  static constexpr const uint8_t kCebHeaderSigLen = 8;

  struct Header {
    char sig[kCebHeaderSigLen];
    // numStoredEntries will be stored in the buffer as the part of the header.
    // numStoredEntries can be different from storedKeys_.size()
    // (When the same entry has to be re-written to the different position)
    uint32_t numStoredEntries;
  };

  // Buffer will be allocated only when this CombinedEntryBlock doesn't
  // represent the data in other given buffer.
  // If this CombinedEntryBlock uses the buffer given via the constructor,
  // allocated_ will be nullptr and not used.
  std::unique_ptr<uint8_t[]> allocated_{};

  // bufView will always represent the buffer in use for thie CombinedEntryBlock
  // no matter where the buffer is and how it's allocated.
  MutableBufferView bufView_{};

  Header* headerPtr_{nullptr};
  uint16_t numValidEntries_{0};
  // The base addr from the buffer where the list of entryPosInfo is being
  // stored
  uint8_t* entryPosInfoBase_{nullptr};
  // current position grows in reverse direction (from the end to the 0)
  EntryPos curPos_{kDefaultSize};

  // For quick check if it's already stored and where its info is.
  folly::F14FastMap<uint64_t, EntryIdx> storedKeys_{};
  // To maintain all the bids and # of keys stored with this CombinedEntryBlock
  folly::F14FastMap<uint64_t, uint16_t> keysForBids_{};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

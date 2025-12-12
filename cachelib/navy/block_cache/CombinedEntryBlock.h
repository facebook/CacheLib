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

#include "cachelib/navy/block_cache/FixedSizeIndex.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

enum class CombinedEntryStatus : uint8_t {
  kOk = 0,
  kUpdated,
  kFull,
  kNotFound,
};

using EntryPos = uint16_t;
using EntryIdx = uint16_t;
using EntryRecord = FixedSizeIndex::PackedItemRecord;

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
  // Size for the combined entry block which will be used for Index entries
  static constexpr uint32_t kCombinedEntryBlockSize = 4096;

  CombinedEntryBlock() : buffer_(kCombinedEntryBlockSize) {}

  // Adding a index entry as the content of Combined entry block
  CombinedEntryStatus addIndexEntry(uint64_t bid,
                                    uint64_t key,
                                    const EntryRecord& record);

  // Get the index entry for the given key
  folly::Expected<EntryRecord, CombinedEntryStatus> getIndexEntry(uint64_t key);

  // Get the number of entries
  uint16_t getNumEntries() const { return numEntries_; }

  struct EntryPosInfo {
    // TODO: There will be padding here, but we'll probably need additional
    // fields in this struct anyway
    uint64_t bid;
    uint64_t key;
    EntryPos pos;
  };

 private:
  EntryPos getEntryPos(uint16_t keyIdx) const {
    XDCHECK((keyIdx + 1) * sizeof(EntryPosInfo) <= curPos_);
    return (reinterpret_cast<const EntryPosInfo*>(buffer_.data())[keyIdx]).pos;
  }

  EntryPos getEmptySpacePos(uint16_t numEntry) const {
    return sizeof(EntryPosInfo) * numEntry;
  }

  Buffer buffer_;
  uint16_t numEntries_{0};
  // current position grows in reverse direction (from the end to the 0)
  EntryPos curPos_{kCombinedEntryBlockSize};

  // For quick check if it's already stored and where its info is.
  folly::F14FastMap<uint64_t, EntryIdx> storedKeys_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/navy/block_cache/CombinedEntryBlock.h"

namespace facebook {
namespace cachelib {
namespace navy {

CombinedEntryStatus CombinedEntryBlock::addIndexEntry(
    uint64_t bid, uint64_t key, const EntryRecord& record) {
  bool update = false;
  uint16_t keyIdx = numEntries_;

  // Check if the key already exists
  auto it = storedKeys_.find(key);
  if (it != storedKeys_.end()) {
    update = true;
    keyIdx = it->second;
  }

  if (update) {
    EntryPos pos = getEntryPos(keyIdx);
    *reinterpret_cast<EntryRecord*>(buffer_.data() + pos) = record;
    return CombinedEntryStatus::kUpdated;
  } else {
    EntryPos pos = curPos_ - sizeof(EntryRecord);
    if (pos < getEmptySpacePos(numEntries_ + 1)) {
      // There's no space to add more entry here
      return CombinedEntryStatus::kFull;
    }
    *reinterpret_cast<EntryRecord*>(buffer_.data() + pos) = record;
    // Add the entry info for the newly added entry
    reinterpret_cast<EntryPosInfo*>(buffer_.data())[keyIdx] = {bid, key, pos};
    storedKeys_.insert({key, keyIdx});
    numEntries_++;
    curPos_ = pos;
    return CombinedEntryStatus::kOk;
  }
}

folly::Expected<EntryRecord, CombinedEntryStatus>
CombinedEntryBlock::getIndexEntry(uint64_t key) {
  auto it = storedKeys_.find(key);
  if (it == storedKeys_.end()) {
    return folly::makeUnexpected(CombinedEntryStatus::kNotFound);
  }
  return *reinterpret_cast<EntryRecord*>(buffer_.data() +
                                         getEntryPos(it->second));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

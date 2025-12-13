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
  uint16_t keyIdx = numStoredEntries_;

  // Check if the key already exists
  auto it = storedKeys_.find(key);
  if (it != storedKeys_.end()) {
    // There should be an entry if it was stored once even when it's removed
    update = true;
    keyIdx = it->second;
  }

  if (update) {
    EntryPos pos = getEntryPos(keyIdx);
    entryRecordRef(pos) = record;

    auto& posInfo = entryPosInfoRef(keyIdx);
    if (posInfo.flag.removed == 1) {
      // In case it was removed before
      posInfo.flag.removed = 0;
      numValidEntries_++;
      // It's not actually updated since it had been removed before
      return CombinedEntryStatus::kOk;
    }
    return CombinedEntryStatus::kUpdated;
  } else {
    EntryPos pos = curPos_ - sizeof(EntryRecord);
    if (pos < getEmptySpacePos(numStoredEntries_ + 1)) {
      // There's no space to add more entry here
      return CombinedEntryStatus::kFull;
    }
    entryRecordRef(pos) = record;
    // Add the entry info for the newly added entry
    entryPosInfoRef(keyIdx) = {bid, key, pos};
    storedKeys_.insert({key, keyIdx});
    numStoredEntries_++;
    numValidEntries_++;
    curPos_ = pos;
    return CombinedEntryStatus::kOk;
  }
}

folly::Expected<EntryRecord, CombinedEntryStatus>
CombinedEntryBlock::getIndexEntry(uint64_t key) {
  auto it = storedKeys_.find(key);
  if (it == storedKeys_.end() ||
      entryPosInfoRef(it->second).flag.removed == 1) {
    return folly::makeUnexpected(CombinedEntryStatus::kNotFound);
  }

  return entryRecordRef(getEntryPos(it->second));
}

CombinedEntryStatus CombinedEntryBlock::removeIndexEntry(uint64_t key) {
  auto it = storedKeys_.find(key);
  if (it == storedKeys_.end() ||
      entryPosInfoRef(it->second).flag.removed == 1) {
    return CombinedEntryStatus::kNotFound;
  }

  // Removing the entry from the storedKeys map is meaningless and not helpful,
  // since storedKeys map won't be written to the flash, and In-place
  // modification directly to the buffer may cause too much overhead. Also if
  // the same key entry is updated while it's still in memory, we probably want
  // to reuse the previously occupied space. So removed entry will be just
  // marked as removed. (When CombinedEntryBlock was already written to flash,
  // any modification including removing means new write to the different
  // CombinedEntryBlock anyway, so handling will be different for that)
  entryPosInfoRef(it->second).flag.removed = 1;
  numValidEntries_--;
  return CombinedEntryStatus::kOk;
}

bool CombinedEntryBlock::peekIndexEntry(uint64_t key) {
  auto it = storedKeys_.find(key);
  return (it != storedKeys_.end() &&
          entryPosInfoRef(it->second).flag.removed == 0);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

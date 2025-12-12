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

#include <gtest/gtest.h>

#include "cachelib/navy/block_cache/CombinedEntryBlock.h"

namespace facebook::cachelib::navy::tests {

TEST(CombinedEntryBlockTest, AddIndexEntry) {
  CombinedEntryBlock combinedBlk;

  // Initial state
  EXPECT_EQ(combinedBlk.getNumEntries(), 0);

  // Add entries
  FixedSizeIndex::PackedItemRecord rec1{100, 10, 1};
  uint64_t key1 = 0;
  auto res = combinedBlk.addIndexEntry(0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  FixedSizeIndex::PackedItemRecord rec2{200, 10, 1};
  uint64_t key2 = 1;
  res = combinedBlk.addIndexEntry(0, key2, rec2);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Check entries
  EXPECT_EQ(combinedBlk.getNumEntries(), 2);

  auto entry1 = combinedBlk.getIndexEntry(key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);

  auto entry2 = combinedBlk.getIndexEntry(key2);
  EXPECT_TRUE(entry2.hasValue());
  EXPECT_EQ(entry2.value(), rec2);

  // Check non-existent entry
  uint64_t key3 = 100;
  auto entry3 = combinedBlk.getIndexEntry(key3);
  EXPECT_FALSE(entry3.hasValue());
  EXPECT_EQ(entry3.error(), CombinedEntryStatus::kNotFound);

  // Update the entry
  rec1.address = 2000;
  res = combinedBlk.addIndexEntry(0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kUpdated);

  // Still has the same number of entries
  EXPECT_EQ(combinedBlk.getNumEntries(), 2);
  // Check updated entry
  entry1 = combinedBlk.getIndexEntry(key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);
}

TEST(CombinedEntryBlockTest, AddIndexEntryFull) {
  CombinedEntryBlock combinedBlk;

  // Initial state
  EXPECT_EQ(combinedBlk.getNumEntries(), 0);

  // This will be changed in the future
  uint16_t maxNumEntries =
      CombinedEntryBlock::kCombinedEntryBlockSize /
      (sizeof(CombinedEntryBlock::EntryPosInfo) + sizeof(EntryRecord));

  for (auto i = 0; i < maxNumEntries; i++) {
    FixedSizeIndex::PackedItemRecord rec{(uint32_t)i + 100, 10, 1};
    uint64_t key = i;
    auto res = combinedBlk.addIndexEntry(0, key, rec);
    EXPECT_EQ(res, CombinedEntryStatus::kOk);
  }

  // Check everything was added properly
  for (auto i = 0; i < maxNumEntries; i++) {
    FixedSizeIndex::PackedItemRecord rec{(uint32_t)i + 100, 10, 1};
    uint64_t key = i;
    auto entry = combinedBlk.getIndexEntry(key);
    EXPECT_TRUE(entry.hasValue());
    EXPECT_EQ(entry.value(), rec);
  }

  // One more entry will fail with kFull
  FixedSizeIndex::PackedItemRecord rec{maxNumEntries, 10, 1};
  auto res = combinedBlk.addIndexEntry(0, maxNumEntries, rec);
  EXPECT_EQ(res, CombinedEntryStatus::kFull);

  // Check number of entries
  EXPECT_EQ(combinedBlk.getNumEntries(), maxNumEntries);
}

} // namespace facebook::cachelib::navy::tests

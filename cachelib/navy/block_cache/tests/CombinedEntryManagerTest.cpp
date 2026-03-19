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

#include "cachelib/navy/block_cache/CombinedEntryManager.h"

namespace facebook::cachelib::navy::tests {

TEST(CombinedEntryManagerTest, getCombinedEntryBlock) {
  CombinedEntryManager combinedEntryMgr(10, 4096, nullptr, "test");

  // Initially zero CEB created
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 0);

  auto combinedBlk = combinedEntryMgr.getCombinedEntryBlock(0);
  EXPECT_NE(combinedBlk, nullptr);
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 1);

  // Given same stream, it should return the same instance
  auto combinedBlk2 = combinedEntryMgr.getCombinedEntryBlock(0);
  EXPECT_EQ(combinedBlk, combinedBlk2);
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 1);
}

TEST(CombinedEntryManagerTest, AddIndexEntry) {
  CombinedEntryManager combinedEntryMgr(10, 4096, nullptr, "test");

  // add index entries to stream 0
  Index::PackedItemRecord rec1{100, 10, 1};
  uint64_t key1 = 0;
  uint64_t stream = 0;
  auto res = combinedEntryMgr.addIndexEntryToStream(stream, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  Index::PackedItemRecord rec2{200, 10, 1};
  uint64_t key2 = 1;
  res = combinedEntryMgr.addIndexEntryToStream(stream, 0, key2, rec2);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Only one CEB created so far
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 1);

  auto combinedBlk0 = combinedEntryMgr.getCombinedEntryBlock(stream);
  EXPECT_EQ(combinedBlk0->getNumStoredEntries(), 2);

  // Check if it's added well
  auto entry1 = combinedBlk0->getIndexEntry(key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);

  auto entry2 = combinedBlk0->getIndexEntry(key2);
  EXPECT_TRUE(entry2.hasValue());
  EXPECT_EQ(entry2.value(), rec2);

  // Add a index entry to another stream
  Index::PackedItemRecord rec3{300, 10, 1};
  uint64_t key3 = 2;
  uint64_t stream1 = 1;
  res = combinedEntryMgr.addIndexEntryToStream(stream1, 0, key3, rec3);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Now there are two CEBs
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 2);

  // Check if it's added well
  auto combinedBlk1 = combinedEntryMgr.getCombinedEntryBlock(stream1);
  EXPECT_EQ(combinedBlk1->getNumStoredEntries(), 1);

  auto entry3 = combinedBlk1->getIndexEntry(key3);
  EXPECT_TRUE(entry3.hasValue());
  EXPECT_EQ(entry3.value(), rec3);

  // Entry was added to the designated stream only
  EXPECT_FALSE(combinedBlk0->peekIndexEntry(key3));
  EXPECT_FALSE(combinedBlk1->peekIndexEntry(key1));
  EXPECT_FALSE(combinedBlk1->peekIndexEntry(key2));
}

TEST(CombinedEntryManagerTest, RemoveIndexEntry) {
  CombinedEntryManager combinedEntryMgr(10, 4096, nullptr, "test");

  // Initial state
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 0);

  // Add an entry
  Index::PackedItemRecord rec1{100, 10, 1};
  uint64_t key1 = 0;
  uint64_t stream1 = 0;
  auto res = combinedEntryMgr.addIndexEntryToStream(stream1, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 1);

  // Check the added entry
  EXPECT_TRUE(combinedEntryMgr.peekIndexEntryFromStream(stream1, key1));
  EXPECT_FALSE(combinedEntryMgr.peekIndexEntryFromStream(stream1 + 1, key1));

  auto entry1 = combinedEntryMgr.getIndexEntryFromStream(stream1, key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);

  res = combinedEntryMgr.removeIndexEntryFromStream(stream1, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Check the removed entry
  entry1 = combinedEntryMgr.getIndexEntryFromStream(stream1, key1);
  EXPECT_FALSE(entry1.hasValue());
  EXPECT_EQ(entry1.error(), CombinedEntryStatus::kNotFound);
  EXPECT_FALSE(combinedEntryMgr.peekIndexEntryFromStream(stream1, key1));

  // Created combined entry block for the stream won't be destroyed
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 1);

  // Add another entry
  Index::PackedItemRecord rec2{200, 10, 1};
  uint64_t key2 = 1;
  uint64_t stream2 = 1;
  res = combinedEntryMgr.addIndexEntryToStream(stream2, 0, key2, rec2);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  EXPECT_EQ(combinedEntryMgr.getTotalCombinedEntryBlocks(), 2);

  // Check the added entry
  EXPECT_TRUE(combinedEntryMgr.peekIndexEntryFromStream(stream2, key2));

  // Remove non-existing entry
  res = combinedEntryMgr.removeIndexEntryFromStream(stream2, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kNotFound);
  // Remove already removed entry
  res = combinedEntryMgr.removeIndexEntryFromStream(stream1, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kNotFound);

  // Add the same key for the removed entry again
  rec1.address = 2000;
  res = combinedEntryMgr.addIndexEntryToStream(stream1, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  // Check the newly added entry
  entry1 = combinedEntryMgr.getIndexEntryFromStream(stream1, key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);
}
} // namespace facebook::cachelib::navy::tests

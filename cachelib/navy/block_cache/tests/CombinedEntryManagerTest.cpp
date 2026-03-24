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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/block_cache/CombinedEntryManager.h"

namespace facebook::cachelib::navy::tests {

// Interface to create a mocking class only for testing
class HandleWriteCEBToFlashClass {
 public:
  virtual ~HandleWriteCEBToFlashClass() = default;
  virtual Status onWriteCombinedEntryBlock(uint64_t stream,
                                           const CombinedEntryBlock& ceb) = 0;
};

class MockWriteCEBToFlash : public HandleWriteCEBToFlashClass {
 public:
  MOCK_METHOD2(onWriteCombinedEntryBlock,
               Status(uint64_t, const CombinedEntryBlock&));
};

class CombinedEntryManagerTest : public testing::Test {
 public:
  void SetUp() override {
    combinedEntryMgr_ = std::make_unique<CombinedEntryManager>(
        10,
        4096,
        nullptr,
        "test",
        bindThis(&MockWriteCEBToFlash::onWriteCombinedEntryBlock,
                 mockCEBWriter_));
  }

 protected:
  std::unique_ptr<CombinedEntryManager> combinedEntryMgr_;
  MockWriteCEBToFlash mockCEBWriter_;
};

TEST_F(CombinedEntryManagerTest, getCombinedEntryBlock) {
  // Initially zero CEB created
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 0);

  auto combinedBlk = combinedEntryMgr_->getCombinedEntryBlock(0);
  EXPECT_NE(combinedBlk, nullptr);
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);

  // Given same stream, it should return the same instance
  auto combinedBlk2 = combinedEntryMgr_->getCombinedEntryBlock(0);
  EXPECT_EQ(combinedBlk, combinedBlk2);
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);
}

TEST_F(CombinedEntryManagerTest, AddIndexEntry) {
  // add index entries to stream 0
  Index::PackedItemRecord rec1{100, 10, 1};
  uint64_t key1 = 0;
  uint64_t stream = 0;
  auto res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  Index::PackedItemRecord rec2{200, 10, 1};
  uint64_t key2 = 1;
  res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key2, rec2);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Only one CEB created so far
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);

  auto combinedBlk0 = combinedEntryMgr_->getCombinedEntryBlock(stream);
  EXPECT_EQ(combinedBlk0->numStoredEntries(), 2);

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
  res = combinedEntryMgr_->addIndexEntryToStream(stream1, 0, key3, rec3);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Now there are two CEBs
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 2);

  // Check if it's added well
  auto combinedBlk1 = combinedEntryMgr_->getCombinedEntryBlock(stream1);
  EXPECT_EQ(combinedBlk1->numStoredEntries(), 1);

  auto entry3 = combinedBlk1->getIndexEntry(key3);
  EXPECT_TRUE(entry3.hasValue());
  EXPECT_EQ(entry3.value(), rec3);

  // Entry was added to the designated stream only
  EXPECT_FALSE(combinedBlk0->peekIndexEntry(key3));
  EXPECT_FALSE(combinedBlk1->peekIndexEntry(key1));
  EXPECT_FALSE(combinedBlk1->peekIndexEntry(key2));
}

TEST_F(CombinedEntryManagerTest, RemoveIndexEntry) {
  // Initial state
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 0);

  // Add an entry
  Index::PackedItemRecord rec1{100, 10, 1};
  uint64_t key1 = 0;
  uint64_t stream1 = 0;
  auto res = combinedEntryMgr_->addIndexEntryToStream(stream1, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);

  // Check the added entry
  EXPECT_TRUE(combinedEntryMgr_->peekIndexEntryFromStream(stream1, key1));
  EXPECT_FALSE(combinedEntryMgr_->peekIndexEntryFromStream(stream1 + 1, key1));

  auto entry1 = combinedEntryMgr_->getIndexEntryFromStream(stream1, key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);

  res = combinedEntryMgr_->removeIndexEntryFromStream(stream1, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  // Check the removed entry
  entry1 = combinedEntryMgr_->getIndexEntryFromStream(stream1, key1);
  EXPECT_FALSE(entry1.hasValue());
  EXPECT_EQ(entry1.error(), CombinedEntryStatus::kNotFound);
  EXPECT_FALSE(combinedEntryMgr_->peekIndexEntryFromStream(stream1, key1));

  // Created combined entry block for the stream won't be destroyed
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);

  // Add another entry
  Index::PackedItemRecord rec2{200, 10, 1};
  uint64_t key2 = 1;
  uint64_t stream2 = 1;
  res = combinedEntryMgr_->addIndexEntryToStream(stream2, 0, key2, rec2);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 2);

  // Check the added entry
  EXPECT_TRUE(combinedEntryMgr_->peekIndexEntryFromStream(stream2, key2));

  // Remove non-existing entry
  res = combinedEntryMgr_->removeIndexEntryFromStream(stream2, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kNotFound);
  // Remove already removed entry
  res = combinedEntryMgr_->removeIndexEntryFromStream(stream1, key1);
  EXPECT_EQ(res, CombinedEntryStatus::kNotFound);

  // Add the same key for the removed entry again
  rec1.address = 2000;
  res = combinedEntryMgr_->addIndexEntryToStream(stream1, 0, key1, rec1);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  // Check the newly added entry
  entry1 = combinedEntryMgr_->getIndexEntryFromStream(stream1, key1);
  EXPECT_TRUE(entry1.hasValue());
  EXPECT_EQ(entry1.value(), rec1);
}

TEST_F(CombinedEntryManagerTest, FullyFilledThenWriteIssued) {
  // Initial state
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 0);

  // Adding an index entry will create a combined entry block
  Index::PackedItemRecord rec{100, 10, 1};
  uint64_t key = 0;
  uint64_t stream = 0;
  auto res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key, rec);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);
  EXPECT_EQ(combinedEntryMgr_->getTotalCombinedEntryBlocks(), 1);

  auto combinedBlk0 = combinedEntryMgr_->getCombinedEntryBlock(stream);
  EXPECT_EQ(combinedBlk0->numStoredEntries(), 1);

  // Calculate possible max index entries for this combined block
  uint16_t maxNumEntries =
      combinedBlk0->getUsableSize() /
      (sizeof(CombinedEntryBlock::EntryPosInfo) + sizeof(EntryRecord));

  EXPECT_CALL(mockCEBWriter_, onWriteCombinedEntryBlock(testing::_, testing::_))
      .Times(0);
  for (auto i = 1; i < maxNumEntries; i++) {
    rec = {(uint32_t)i + 100, 10, 1};
    key = i;

    res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key, rec);
    EXPECT_EQ(res, CombinedEntryStatus::kOk);
  }

  // Now adding more index entry will trigger writing it to flash
  EXPECT_CALL(mockCEBWriter_, onWriteCombinedEntryBlock(testing::_, testing::_))
      .WillOnce(testing::Return(Status::Ok))
      .WillOnce(testing::Return(Status::Retry));
  rec = {(uint32_t)maxNumEntries + 100, 10, 1};
  key = maxNumEntries;
  res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key, rec);
  EXPECT_EQ(res, CombinedEntryStatus::kOk);

  EXPECT_EQ(combinedBlk0->numStoredEntries(), 1);

  // Let's repeat it and this time WriteCebCb will return Retry, so we will get
  // CombinedEntryStatus::kFull
  for (auto i = 1; i < maxNumEntries; i++) {
    rec = {(uint32_t)i + 100, 10, 1};
    key = i;

    res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key, rec);
    EXPECT_EQ(res, CombinedEntryStatus::kOk);
  }
  rec = {(uint32_t)maxNumEntries + 100, 10, 1};
  key = maxNumEntries + 1;
  res = combinedEntryMgr_->addIndexEntryToStream(stream, 0, key, rec);
  EXPECT_EQ(res, CombinedEntryStatus::kFull);
  EXPECT_EQ(combinedBlk0->numStoredEntries(), maxNumEntries);
}
} // namespace facebook::cachelib::navy::tests

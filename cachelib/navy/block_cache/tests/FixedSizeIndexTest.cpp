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

#include <thread>

#define FixedSizeIndex_TEST_FRIENDS_FORWARD_DECLARATION \
  namespace tests {                                     \
  class FixedSizeIndex_MemFootprintRangeTest_Test;      \
  class FixedSizeIndex_Hits_Test;                       \
  }                                                     \
  using namespace ::facebook::cachelib::navy::tests

#define FixedSizeIndex_TEST_FRIENDS                   \
  FRIEND_TEST(FixedSizeIndex, MemFootprintRangeTest); \
  FRIEND_TEST(FixedSizeIndex, Hits)

#include "cachelib/navy/block_cache/FixedSizeIndex.h"
#include "cachelib/navy/testing/MockDevice.h"

namespace facebook::cachelib::navy::tests {
TEST(FixedSizeIndex, Recovery) {
  FixedSizeIndex index{1, 8, 16};
  std::vector<std::pair<uint64_t, uint32_t>> log;
  // There won't be hash collision with these keys
  for (uint32_t i = 0; i < 128; i++) {
    uint64_t key = i;
    uint32_t val = i + 1;
    index.insert(key, val, 100);
    log.emplace_back(key, val);
  }

  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);
  index.persist(*rw);

  auto rr = createMemoryRecordReader(ioq);
  FixedSizeIndex newIndex{1, 8, 16};
  newIndex.recover(*rr);
  for (auto& entry : log) {
    auto lookupResult = newIndex.lookup(entry.first);
    EXPECT_EQ(entry.second, lookupResult.address());
  }
}

TEST(FixedSizeIndex, EntrySize) {
  FixedSizeIndex index{1, 9, 16};
  // Since FixedSizeIndex store sizeHint as one of the predefined values,
  // FixedSizeIndex will store sizeHint which may be larger than the sizeHint
  // given by the caller
  index.insert(111, 100, 11);
  EXPECT_EQ(100, index.lookup(111).address());
  EXPECT_LE(11, index.lookup(111).sizeHint());
  index.insert(222, 200, 150);
  EXPECT_EQ(200, index.lookup(222).address());
  EXPECT_LE(150, index.lookup(222).sizeHint());
  index.insert(333, 300, 303);
  EXPECT_EQ(300, index.lookup(333).address());
  EXPECT_LE(303, index.lookup(333).sizeHint());
}

TEST(FixedSizeIndex, ReplaceExact) {
  FixedSizeIndex index{1, 9, 16};
  // Empty value should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_FALSE(index.lookup(111).found());

  index.insert(111, 4444, 123);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());
  EXPECT_LE(123, index.lookup(111).sizeHint());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());
  EXPECT_LE(123, index.lookup(111).sizeHint());

  EXPECT_TRUE(index.replaceIfMatch(111, 3333, 4444));
  EXPECT_EQ(3333, index.lookup(111).address());
}

TEST(FixedSizeIndex, RemoveExact) {
  FixedSizeIndex index{1, 9, 16};
  // Empty value should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 4444));

  index.insert(111, 4444, 123);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());

  EXPECT_TRUE(index.removeIfMatch(111, 4444));
  EXPECT_FALSE(index.lookup(111).found());
}

TEST(FixedSizeIndex, Hits) {
  FixedSizeIndex index{1, 9, 16};
  const uint64_t key = 500;
  // FixedSizeIndex doesn't track total hits

  // Hits after inserting should be 0
  index.insert(key, 100, 150);
  EXPECT_EQ(0, index.peek(key).currentHits());

  // Hits after lookup should increase
  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).currentHits());

  index.setHitsTestOnly(key, 2, 0);
  EXPECT_EQ(2, index.peek(key).currentHits());

  index.lookup(key);
  EXPECT_EQ(3, index.peek(key).currentHits());

  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());

  // removing a second time is fine. Just no-op
  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());
}

TEST(FixedSizeIndex, HitsAfterUpdate) {
  FixedSizeIndex index{1, 9, 16};
  const uint64_t key = 9527;
  // FixedSizeIndex doesn't track total hits

  // Hits after inserting should be 0
  index.insert(key, 100, 150);
  EXPECT_EQ(0, index.peek(key).currentHits());

  // Hits after lookup should increase
  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).currentHits());

  // re-insert
  index.insert(key, 3, 200);
  // hits should be cleared after insert
  EXPECT_EQ(0, index.peek(key).currentHits());

  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).currentHits());

  EXPECT_FALSE(index.replaceIfMatch(key, 200, 100));
  // hits should not be cleared after failed replaceIfMatch()
  EXPECT_EQ(1, index.peek(key).currentHits());

  EXPECT_TRUE(index.replaceIfMatch(key, 100, 3));
  // After success replaceIfMatch(), current hits
  // should be cleared
  EXPECT_EQ(0, index.peek(key).currentHits());
}

TEST(FixedSizeIndex, HitsUpperBound) {
  FixedSizeIndex index{1, 8, 16};
  const uint64_t key = 8341;

  index.insert(key, 100, 200);
  for (int i = 0; i < 1000; i++) {
    index.lookup(key);
  }

  EXPECT_EQ(3, index.peek(key).currentHits());
}

TEST(FixedSizeIndex, ThreadSafe) {
  FixedSizeIndex index{1, 8, 16};
  const uint64_t key = 1314;
  index.insert(key, 123, 200);

  auto lookup = [&]() {
    auto lr = index.lookup(key);
    EXPECT_TRUE(lr.found());
    EXPECT_EQ(123, lr.address());
  };

  std::vector<std::thread> threads;
  threads.reserve(200);
  for (int i = 0; i < 200; i++) {
    threads.emplace_back(lookup);
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(3, index.peek(key).currentHits());
}

TEST(FixedSizeIndex, MemFootprintRangeTest) {
  // With FixedSizeIndex, memory consumption is determined by configured number
  // of buckets and mutexes and those memory will be pre-allocated.
  FixedSizeIndex index{1, 8, 16};
  auto rangeEmpty = index.computeMemFootprintRange();

  // It's always fixed size, min/max should be the same
  EXPECT_EQ(rangeEmpty.minUsedBytes, rangeEmpty.maxUsedBytes);

  // It should be at least larger than (# of buckets) * sizeof(bucket)
  size_t htSize =
      index.totalBuckets_ * sizeof(FixedSizeIndex::PackedItemRecord);
  EXPECT_GT(rangeEmpty.maxUsedBytes, htSize);

  size_t sizeHint = 100;
  index.insert(1 /* random key */, 100 /* random addr */, sizeHint);

  auto range = index.computeMemFootprintRange();
  // It should be same fixed size
  EXPECT_EQ(range.minUsedBytes, rangeEmpty.minUsedBytes);
  EXPECT_EQ(range.maxUsedBytes, rangeEmpty.maxUsedBytes);

  for (int i = 0; i < 100; i++) {
    index.insert(i, i + 100, sizeHint);
    // make sure it's added to index properly
    EXPECT_LE(sizeHint, index.lookup(i).sizeHint());
    EXPECT_EQ(i + 100, index.lookup(i).address());
  }
  range = index.computeMemFootprintRange();
  EXPECT_EQ(range.minUsedBytes, rangeEmpty.minUsedBytes);
  EXPECT_EQ(range.maxUsedBytes, rangeEmpty.maxUsedBytes);
}

TEST(FixedSizeIndex, Reset) {
  FixedSizeIndex index{1, 8, 16};

  // Insert some items
  for (int i = 0; i < 100; i++) {
    index.insert(i, i + 100, 200);
    // Verify items are in the index
    EXPECT_TRUE(index.lookup(i).found());
    EXPECT_EQ(i + 100, index.lookup(i).address());
  }

  // Reset the index
  index.reset();

  // Verify all items are removed
  for (int i = 0; i < 100; i++) {
    EXPECT_FALSE(index.lookup(i).found());
  }
}

TEST(FixedSizeIndex, ComputeSize) {
  FixedSizeIndex index{1, 8, 16};

  // Initially the size should be 0
  EXPECT_EQ(0, index.computeSize());

  // Insert some items
  const int numItems = 50;
  for (int i = 0; i < numItems; i++) {
    index.insert(i, i + 100, 200);
    // Verify items are in the index
    EXPECT_TRUE(index.lookup(i).found());
  }

  // Verify the size matches the number of items inserted
  EXPECT_EQ(numItems, index.computeSize());

  // Remove some items
  const int numToRemove = 20;
  for (int i = 0; i < numToRemove; i++) {
    index.remove(i);
    EXPECT_FALSE(index.lookup(i).found());
  }

  // Verify the size is updated correctly
  EXPECT_EQ(numItems - numToRemove, index.computeSize());
}

} // namespace facebook::cachelib::navy::tests

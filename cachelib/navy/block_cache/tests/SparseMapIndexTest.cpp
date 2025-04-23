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

#define SparseMapIndex_TEST_FRIENDS_FORWARD_DECLARATION \
  namespace tests {                                     \
  class SparseMapIndex_MemFootprintRangeTest_Test;      \
  }                                                     \
  using namespace ::facebook::cachelib::navy::tests

#define SparseMapIndex_TEST_FRIENDS \
  FRIEND_TEST(SparseMapIndex, MemFootprintRangeTest)

#include "cachelib/navy/block_cache/SparseMapIndex.h"
#include "cachelib/navy/testing/MockDevice.h"

namespace facebook::cachelib::navy::tests {
TEST(SparseMapIndex, Recovery) {
  SparseMapIndex index;
  std::vector<std::pair<uint64_t, uint32_t>> log;
  // Write to 16 buckets
  for (uint64_t i = 0; i < 16; i++) {
    for (uint64_t j = 0; j < 10; j++) {
      // First 32 bits set bucket id, last 32 is key for that bucket
      uint64_t key = i << 32 | j;
      uint32_t val = j + i;
      index.insert(key, val, 0);
      log.emplace_back(key, val);
    }
  }

  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);
  index.persist(*rw);

  auto rr = createMemoryRecordReader(ioq);
  SparseMapIndex newIndex;
  newIndex.recover(*rr);
  for (auto& entry : log) {
    auto lookupResult = newIndex.lookup(entry.first);
    EXPECT_EQ(entry.second, lookupResult.address());
  }
}

TEST(SparseMapIndex, EntrySize) {
  SparseMapIndex index;
  index.insert(111, 0, 11);
  EXPECT_EQ(11, index.lookup(111).sizeHint());
  index.insert(222, 0, 150);
  EXPECT_EQ(150, index.lookup(222).sizeHint());
  index.insert(333, 0, 303);
  EXPECT_EQ(303, index.lookup(333).sizeHint());
}

TEST(SparseMapIndex, ReplaceExact) {
  SparseMapIndex index;
  // Empty value should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_FALSE(index.lookup(111).found());

  index.insert(111, 4444, 123);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());
  EXPECT_EQ(123, index.lookup(111).sizeHint());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());
  EXPECT_EQ(123, index.lookup(111).sizeHint());

  EXPECT_TRUE(index.replaceIfMatch(111, 3333, 4444));
  EXPECT_EQ(3333, index.lookup(111).address());
}

TEST(SparseMapIndex, RemoveExact) {
  SparseMapIndex index;
  // Empty value should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 4444));

  index.insert(111, 4444, 0);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());

  EXPECT_TRUE(index.removeIfMatch(111, 4444));
  EXPECT_FALSE(index.lookup(111).found());
}

TEST(SparseMapIndex, Hits) {
  SparseMapIndex index;
  const uint64_t key = 9527;

  // Hits after inserting should be 0
  index.insert(key, 0, 0);
  EXPECT_EQ(0, index.peek(key).totalHits());
  EXPECT_EQ(0, index.peek(key).currentHits());

  // Hits after lookup should increase
  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).totalHits());
  EXPECT_EQ(1, index.peek(key).currentHits());

  index.setHits(key, 2, 5);
  EXPECT_EQ(5, index.peek(key).totalHits());
  EXPECT_EQ(2, index.peek(key).currentHits());

  index.lookup(key);
  EXPECT_EQ(6, index.peek(key).totalHits());
  EXPECT_EQ(3, index.peek(key).currentHits());

  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());

  // removing a second time is fine. Just no-op
  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());
}

TEST(SparseMapIndex, HitsAfterUpdate) {
  SparseMapIndex index;
  const uint64_t key = 9527;

  // Hits after inserting should be 0
  index.insert(key, 0, 0);
  EXPECT_EQ(0, index.peek(key).totalHits());
  EXPECT_EQ(0, index.peek(key).currentHits());

  // Hits after lookup should increase
  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).totalHits());
  EXPECT_EQ(1, index.peek(key).currentHits());

  // re-insert
  index.insert(key, 3, 0);
  // hits should be cleared after insert
  EXPECT_EQ(0, index.peek(key).totalHits());
  EXPECT_EQ(0, index.peek(key).currentHits());

  index.lookup(key);
  EXPECT_EQ(1, index.peek(key).totalHits());
  EXPECT_EQ(1, index.peek(key).currentHits());

  EXPECT_FALSE(index.replaceIfMatch(key, 100, 0));
  // hits should not be cleared after failed replaceIfMatch()
  EXPECT_EQ(1, index.peek(key).totalHits());
  EXPECT_EQ(1, index.peek(key).currentHits());

  EXPECT_TRUE(index.replaceIfMatch(key, 100, 3));
  // After success replaceIfMatch(), totalHits should be kept but currentHits
  // should be cleared
  EXPECT_EQ(1, index.peek(key).totalHits());
  EXPECT_EQ(0, index.peek(key).currentHits());
}

TEST(SparseMapIndex, HitsUpperBound) {
  SparseMapIndex index;
  const uint64_t key = 8341;

  index.insert(key, 0, 0);
  for (int i = 0; i < 1000; i++) {
    index.lookup(key);
  }

  EXPECT_EQ(255, index.peek(key).totalHits());
  EXPECT_EQ(255, index.peek(key).currentHits());
}

TEST(SparseMapIndex, ThreadSafe) {
  SparseMapIndex index;
  const uint64_t key = 1314;
  index.insert(key, 0, 0);

  auto lookup = [&]() { index.lookup(key); };

  std::vector<std::thread> threads;
  threads.reserve(200);
  for (int i = 0; i < 200; i++) {
    threads.emplace_back(lookup);
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(200, index.peek(key).totalHits());
  EXPECT_EQ(200, index.peek(key).currentHits());
}

TEST(SparseMapIndex, MemFootprintRangeTest) {
  // Though it's not possible to test the exact size of the index without some
  // hard coded number which is not a good idea, we can do some minimal testings
  // to make sure the memory footprint for clear cases are computed properly.
  SparseMapIndex index;
  auto range = index.computeMemFootprintRange();

  // with the empty index, the range should be fixed size which is needed for
  // the structure itself
  auto baseSize = range.maxUsedBytes;
  // no difference between min and max with empty index
  EXPECT_EQ(range.maxUsedBytes, range.minUsedBytes);
  EXPECT_EQ(baseSize,
            SparseMapIndex::kNumBuckets * sizeof(SparseMapIndex::Map));
  EXPECT_GT(baseSize, 0);

  // just a random number
  size_t sizeHint = 100;
  auto indexEntrySize =
      sizeof(std::pair<typename SparseMapIndex::Map::key_type,
                       typename SparseMapIndex::Map::value_type>);
  index.insert(1 /* random key */, 100 /* random addr */, sizeHint);

  range = index.computeMemFootprintRange();
  // now the memmory footprint should be at least larger than the base size
  // (empty index) + one entry's payload size
  EXPECT_GT(range.minUsedBytes, baseSize + indexEntrySize);
  // with only one entry added, the min and max should be the same
  EXPECT_EQ(range.maxUsedBytes, range.minUsedBytes);

  for (int i = 0; i < 1000; i++) {
    index.insert(i, i + 100, sizeHint);
    // make sure it's added to index properly
    EXPECT_EQ(sizeHint, index.lookup(i).sizeHint());
    EXPECT_EQ(i + 100, index.lookup(i).address());
  }
  range = index.computeMemFootprintRange();

  // now the memory footprint should be at least larger than the base size
  // (empty index) + 1000 * entry's payload size
  EXPECT_GT(range.minUsedBytes, baseSize + 1000 * indexEntrySize);
  // with 1000 entries added, the min and max will not be the same since the
  // sparse_map will consume memory depending on the hash distribution and
  // computeMemFootprintRange() will return mem consumed for the best and the
  // worst cases
  EXPECT_NE(range.maxUsedBytes, range.minUsedBytes);
}

TEST(SparseMapIndex, PersistFailureTest) {
  SparseMapIndex index;

  size_t numEntries = 1000;
  for (uint64_t i = 0; i < numEntries; i++) {
    // Randomly put some entries into the index, but let's make it distributed
    // across buckets
    index.insert(i << 32, i + 100, 100);
  }

  // create a mock device with much smaller size intentionally
  size_t devSize = 2 * 4096;
  MockDevice device(devSize, 1);
  auto rw = createMetadataRecordWriter(device, devSize);

  EXPECT_THROW(index.persist(*rw), std::logic_error);
}

} // namespace facebook::cachelib::navy::tests

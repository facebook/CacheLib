#include <thread>

#include "cachelib/navy/block_cache/Index.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(Index, Recovery) {
  Index index;
  std::vector<std::pair<uint64_t, uint32_t>> log;
  // Write to 16 buckets
  for (uint64_t i = 0; i < 16; i++) {
    for (uint64_t j = 0; j < 10; j++) {
      // First 32 bits set bucket id, last 32 is key for that bucket
      uint64_t key = i << 32 | j;
      uint32_t val = j + i;
      index.insert(key, val);
      log.push_back(std::make_pair(key, val));
    }
  }

  folly::IOBufQueue ioq;
  auto rw = createMemoryRecordWriter(ioq);
  index.persist(*rw);

  auto rr = createMemoryRecordReader(ioq);
  Index newIndex;
  newIndex.recover(*rr);
  for (auto& entry : log) {
    auto lookupResult = newIndex.lookup(entry.first);
    EXPECT_EQ(entry.second, lookupResult.address());
  }
}

TEST(Index, ReplaceExact) {
  Index index;
  // Empty value should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_FALSE(index.lookup(111).found());

  index.insert(111, 4444);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.replaceIfMatch(111, 3333, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());

  EXPECT_TRUE(index.replaceIfMatch(111, 3333, 4444));
  EXPECT_EQ(3333, index.lookup(111).address());
}

TEST(Index, RemoveExact) {
  Index index;
  // Empty value should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 4444));

  index.insert(111, 4444);
  EXPECT_TRUE(index.lookup(111).found());
  EXPECT_EQ(4444, index.lookup(111).address());

  // Old value mismatch should fail in replace
  EXPECT_FALSE(index.removeIfMatch(111, 2222));
  EXPECT_EQ(4444, index.lookup(111).address());

  EXPECT_TRUE(index.removeIfMatch(111, 4444));
  EXPECT_FALSE(index.lookup(111).found());
}

TEST(Index, Hits) {
  Index index;
  const uint64_t key = 9527;

  // Hits after inserting should be 0
  index.insert(key, 0);
  EXPECT_EQ(0, index.lookup(key).totalHits());
  EXPECT_EQ(0, index.lookup(key).currentHits());

  // Hits after touch should increase
  index.touch(key);
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(1, index.lookup(key).currentHits());

  index.setCurrentHits(key, 0);
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(0, index.lookup(key).currentHits());

  index.touch(key);
  EXPECT_EQ(2, index.lookup(key).totalHits());
  EXPECT_EQ(1, index.lookup(key).currentHits());

  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());

  // removing a second time is fine. Just no-op
  index.remove(key);
  EXPECT_FALSE(index.lookup(key).found());
}

TEST(Index, HitsAfterUpdate) {
  Index index;
  const uint64_t key = 9527;

  // Hits after inserting should be 0
  index.insert(key, 0);
  EXPECT_EQ(0, index.lookup(key).totalHits());
  EXPECT_EQ(0, index.lookup(key).currentHits());

  // Hits after touch should increase
  index.touch(key);
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(1, index.lookup(key).currentHits());

  // re-insert
  index.insert(key, 3);
  // hits should be cleared after insert
  EXPECT_EQ(0, index.lookup(key).totalHits());
  EXPECT_EQ(0, index.lookup(key).currentHits());

  index.touch(key);
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(1, index.lookup(key).currentHits());

  EXPECT_FALSE(index.replaceIfMatch(key, 100, 0));
  // hits should not be cleared after failed replaceIfMatch()
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(1, index.lookup(key).currentHits());

  EXPECT_TRUE(index.replaceIfMatch(key, 100, 3));
  // After success replaceIfMatch(), totalHits should be kept but currentHits
  // should be cleared
  EXPECT_EQ(1, index.lookup(key).totalHits());
  EXPECT_EQ(0, index.lookup(key).currentHits());
}

TEST(Index, HitsUpperBound) {
  Index index;
  const uint64_t key = 8341;

  index.insert(key, 0);
  for (int i = 0; i < 1000; i++) {
    index.touch(key);
  }

  EXPECT_EQ(255, index.lookup(key).totalHits());
  EXPECT_EQ(255, index.lookup(key).currentHits());
}

TEST(Index, ThreadSafe) {
  Index index;
  const uint64_t key = 1314;
  index.insert(key, 0);

  auto touch = [&]() { index.touch(key); };

  std::vector<std::thread> threads;
  for (int i = 0; i < 200; i++) {
    threads.emplace_back(std::thread(touch));
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(200, index.lookup(key).totalHits());
  EXPECT_EQ(200, index.lookup(key).currentHits());
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

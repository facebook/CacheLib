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
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

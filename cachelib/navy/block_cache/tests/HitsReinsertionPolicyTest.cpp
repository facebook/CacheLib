#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(HitsReinsertionPolicy, Simple) {
  HitsReinsertionPolicy tracker{1};

  // Touch before tracking has no effect
  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  tracker.track(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(1, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  EXPECT_TRUE(tracker.shouldReinsert(makeHK("test_key_1")));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(0, access.currHits);
    EXPECT_EQ(1, access.numReinsertions);
  }

  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(2, access.totalHits);
    EXPECT_EQ(1, access.currHits);
    EXPECT_EQ(1, access.numReinsertions);
  }

  tracker.remove(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  // removing a second time is fine. Just no-op
  tracker.remove(makeHK("test_key_1"));
}

TEST(HitsReinsertionPolicy, UpperBound) {
  HitsReinsertionPolicy tracker{1};
  tracker.track(makeHK("test_key_1"));
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }
}

TEST(HitsReinsertionPolicy, Recovery) {
  HitsReinsertionPolicy tracker{1};
  tracker.track(makeHK("test_key_1"));
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  // persist to memory then recover from it
  folly::IOBufQueue buf;
  auto rw = createMemoryRecordWriter(buf);
  tracker.persist(*rw);
  tracker.reset();

  auto rr = createMemoryRecordReader(buf);
  tracker.recover(*rr);

  // access stats should be the same
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }
}

TEST(HitsReinsertionPolicy, RecoveryCorruptedData) {
  HitsReinsertionPolicy tracker{1};
  tracker.track(makeHK("test_key_1"));
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  // persist to memory then recover from it
  folly::IOBufQueue buf;
  auto rw = createMemoryRecordWriter(buf);
  tracker.persist(*rw);
  tracker.reset();

  // mess up the buf data by removing the size field
  buf.trimStart(sizeof(size_t));

  auto rr = createMemoryRecordReader(buf);
  EXPECT_THROW(tracker.recover(*rr), std::invalid_argument);
}

TEST(HitsReinsertionPolicy, RecoveryCorruptedData2) {
  HitsReinsertionPolicy tracker{1};
  tracker.track(makeHK("test_key_1"));
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  // persist to memory then recover from it
  folly::IOBufQueue buf;
  auto rw = createMemoryRecordWriter(buf);
  tracker.persist(*rw);
  tracker.reset();

  // mess up the buf data
  buf.trimEnd(8);

  auto rr = createMemoryRecordReader(buf);
  EXPECT_THROW(tracker.recover(*rr), std::out_of_range);
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

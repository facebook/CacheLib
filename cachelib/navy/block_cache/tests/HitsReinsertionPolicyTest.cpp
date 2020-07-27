#include <thread>

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/OldHitsReinsertionPolicy.h"
#include "cachelib/navy/serialization/RecordIO.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(HitsReinsertionPolicy, Simple) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  // Touch before inserting has no effect
  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  // Touch after inserting has effect
  index.insert(makeHK("test_key_1").keyHash(), 0);
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(1, access.currHits);
  }

  EXPECT_TRUE(tracker.shouldReinsert(makeHK("test_key_1")));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(1, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  tracker.touch(makeHK("test_key_1"));
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(2, access.totalHits);
    EXPECT_EQ(1, access.currHits);
  }

  tracker.remove(makeHK("test_key_1"));
  index.remove(makeHK("test_key_1").keyHash());
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(0, access.totalHits);
    EXPECT_EQ(0, access.currHits);
  }

  // removing a second time is fine. Just no-op
  tracker.remove(makeHK("test_key_1"));
}

TEST(HitsReinsertionPolicy, UpperBound) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  index.insert(makeHK("test_key_1").keyHash(), 0);
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }
}

TEST(HitsReinsertionPolicy, ThreadSafe) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  index.insert(makeHK("test_key_1").keyHash(), 0);

  auto touch = [&]() { tracker.touch(makeHK("test_key_1")); };

  std::vector<std::thread> threads;
  for (int i = 0; i < 159; i++) {
    threads.emplace_back(std::thread(touch));
  }

  for (auto& t : threads) {
    t.join();
  }

  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(159, access.totalHits);
    EXPECT_EQ(159, access.currHits);
  }
}

TEST(HitsReinsertionPolicy, Recovery) {
  Index index;
  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  index.insert(makeHK("test_key_1").keyHash(), 0);
  for (int i = 0; i < 1000; i++) {
    tracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }

  // persist to memory then recover from it
  folly::IOBufQueue buf;
  auto rw = createMemoryRecordWriter(buf);
  index.persist(*rw);
  index.reset();

  auto rr = createMemoryRecordReader(buf);
  index.recover(*rr);

  // access stats should be the same
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }
}

TEST(HitsReinsertionPolicy, RecoveryFromOldPolicy) {
  // simulate a warm roll and test new policy recover from old policy's data

  // 1. prepare data
  Index index;
  index.insert(makeHK("test_key_1").keyHash(), 0);

  OldHitsReinsertionPolicy oldTracker{1};
  oldTracker.track(makeHK("test_key_1"));
  for (int i = 0; i < 1000; i++) {
    oldTracker.touch(makeHK("test_key_1"));
  }
  {
    auto access = oldTracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
    EXPECT_EQ(0, access.numReinsertions);
  }

  // 2. let index and old tracker persist
  folly::IOBufQueue buf1;
  auto rw1 = createMemoryRecordWriter(buf1);
  index.persist(*rw1);
  index.reset();

  folly::IOBufQueue buf2;
  auto rw2 = createMemoryRecordWriter(buf2);
  oldTracker.persist(*rw2);
  oldTracker.reset();

  HitsReinsertionPolicy tracker{1};
  tracker.setIndex(&index);

  auto rr1 = createMemoryRecordReader(buf1);
  index.recover(*rr1);

  auto rr2 = createMemoryRecordReader(buf2);
  tracker.recover(*rr2);

  // access stats should be the same
  {
    auto access = tracker.getAccessStats(makeHK("test_key_1"));
    EXPECT_EQ(255, access.totalHits);
    EXPECT_EQ(255, access.currHits);
  }
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

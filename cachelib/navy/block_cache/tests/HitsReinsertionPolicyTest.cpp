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

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {

TEST(HitsReinsertionPolicy, Simple) {
  Index index;
  HitsReinsertionPolicy tracker{1, index};

  auto hk1 = makeHK("test_key_1");
  folly::StringPiece strKey{reinterpret_cast<const char*>(hk1.key().data()),
                            hk1.key().size()};

  // lookup before inserting has no effect
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_FALSE(lr.found());
  }

  // lookup after inserting has effect
  index.insert(hk1.keyHash(), 0, 0);
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(0, lr.totalHits());
    EXPECT_EQ(0, lr.currentHits());
  }

  index.lookup(hk1.keyHash());
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(1, lr.totalHits());
    EXPECT_EQ(1, lr.currentHits());
  }

  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_TRUE(tracker.shouldReinsert(strKey));
    EXPECT_EQ(1, lr.totalHits());
    EXPECT_EQ(1, lr.currentHits());
  }

  // lookup again
  index.lookup(hk1.keyHash());
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(2, lr.totalHits());
    EXPECT_EQ(2, lr.currentHits());
  }

  index.remove(hk1.keyHash());
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_FALSE(lr.found());
  }

  // removing a second time is fine. Just no-op
  index.remove(hk1.keyHash());
}

TEST(HitsReinsertionPolicy, UpperBound) {
  Index index;
  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);
  for (int i = 0; i < 1000; i++) {
    index.lookup(hk1.keyHash());
  }
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(255, lr.totalHits());
    EXPECT_EQ(255, lr.currentHits());
  }
}

TEST(HitsReinsertionPolicy, ThreadSafe) {
  Index index;

  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);

  auto lookup = [&]() { index.lookup(hk1.keyHash()); };

  std::vector<std::thread> threads;
  for (int i = 0; i < 159; i++) {
    threads.emplace_back(std::thread(lookup));
  }

  for (auto& t : threads) {
    t.join();
  }

  {
    auto lr = index.peek(hk1.keyHash());

    EXPECT_EQ(159, lr.totalHits());
    EXPECT_EQ(159, lr.currentHits());
  }
}

TEST(HitsReinsertionPolicy, Recovery) {
  Index index;
  auto hk1 = makeHK("test_key_1");

  index.insert(hk1.keyHash(), 0, 0);
  for (int i = 0; i < 1000; i++) {
    index.lookup(hk1.keyHash());
  }
  {
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(255, lr.totalHits());
    EXPECT_EQ(255, lr.currentHits());
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
    auto lr = index.peek(hk1.keyHash());
    EXPECT_EQ(255, lr.totalHits());
    EXPECT_EQ(255, lr.currentHits());
  }
}

} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

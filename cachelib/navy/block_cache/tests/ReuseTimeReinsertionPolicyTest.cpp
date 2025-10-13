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

#include "cachelib/navy/block_cache/FixedSizeIndex.h"
#include "cachelib/navy/block_cache/ReuseTimeReinsertionPolicy.h"

namespace facebook::cachelib::navy::tests {

class NumberTicker : public Ticker {
 public:
  void setTicks(uint32_t ticks) { ticks_ = ticks; }
  void advanceTicks() { ticks_++; }
  virtual uint32_t getCurrentTick() override { return ticks_; }

 private:
  uint32_t ticks_{0};
};

class ReuseTimeReinsertionPolicyTest : public ::testing::Test {
 protected:
  static constexpr uint32_t kNumChunks = 16;
  static constexpr uint8_t kNumBucketsPerChunkPower = 16;
  static constexpr uint64_t kNumEntriesPerBucketPower = 256;
  static constexpr uint32_t kNumBuckets = 6;
  static constexpr uint32_t kBucketSize = 1;
  static constexpr uint32_t kReuseThreshold = 5;

  void advanceTicks() { ticker_->advanceTicks(); }

  ReuseTimeReinsertionPolicy createPolicy() {
    FixedSizeIndex index{kNumChunks, kNumBucketsPerChunkPower,
                         kNumEntriesPerBucketPower};

    return ReuseTimeReinsertionPolicy(index, kNumBuckets, kBucketSize,
                                      kReuseThreshold, ticker_);
  }

 private:
  std::shared_ptr<NumberTicker> ticker_ = std::make_shared<NumberTicker>();
};

TEST_F(ReuseTimeReinsertionPolicyTest, PrevAccessBucketTracking) {
  // Test to see if the previous bucket access is updated correctly.
  ReuseTimeReinsertionPolicy policy = createPolicy();
  auto kStrKey = "key1";

  // Never seen the key, so no previous access buckets and returns (-1,-1).
  auto prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  ASSERT_EQ(std::get<0>(prevAccessBuckets), -1);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), -1);

  policy.onLookup(kStrKey);

  // Seen this key recently so returns the most recent bucket as 0 but the next
  // most recent bucket as -1 so return (0, -1).
  prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  ASSERT_EQ(std::get<0>(prevAccessBuckets), 0);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), -1);

  advanceTicks();

  // Now that we have advanced the ticks, the most recent bucket is now 1 and
  // the next most recent bucket is still -1 so return (1, -1).
  prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  ASSERT_EQ(std::get<0>(prevAccessBuckets), 1);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), -1);

  policy.onLookup(kStrKey);

  // Now that we have looked up the key again, the most recent bucket would
  // be 0 and the next recent bucket would be 1 so return (0, 1).
  prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  ASSERT_EQ(std::get<0>(prevAccessBuckets), 0);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), 1);

  advanceTicks();

  // Now that we have advanced the ticks, the most recent bucket is now 1 and
  // the next most recent bucket is still 0 so return (1, 2).
  prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  ASSERT_EQ(std::get<0>(prevAccessBuckets), 1);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), 2);

  // Advance the ticks to rotate the access out storage.
  // Buckets are rotated not when ticks advance but when items are accessed
  // so accessing the key after every tick advance to make sure the key
  // is rotated.
  for (uint32_t i = 0; i < kNumBuckets; ++i) {
    advanceTicks();
    prevAccessBuckets = policy.getPrevAccessBuckets(kStrKey);
  }
  ASSERT_EQ(std::get<0>(prevAccessBuckets), -1);
  ASSERT_EQ(std::get<1>(prevAccessBuckets), -1);
}

TEST_F(ReuseTimeReinsertionPolicyTest, ReuseTimeComputation) {
  // Test to see if the reuse time is computed correctly.
  ReuseTimeReinsertionPolicy policy = createPolicy();
  auto kStrKey = "key1";

  // Never accessed so reuse time is 0.
  ASSERT_EQ(policy.getReuseTime(kStrKey), 0);

  policy.onLookup(kStrKey);

  // Now it has been accessed in the most recent bucket so reuse
  // time should be equal to bucket size.
  ASSERT_EQ(policy.getReuseTime(kStrKey), kBucketSize);

  advanceTicks();

  // Now it has been accessed in the second most recent bucket so reuse
  // time should be equal to 2 * bucket size.
  ASSERT_EQ(policy.getReuseTime(kStrKey), 2 * kBucketSize);

  advanceTicks();

  ASSERT_EQ(policy.getReuseTime(kStrKey), 3 * kBucketSize);

  // Now lets access the key again. The reuse time will not be time since
  // last access but the last known reuse time.
  policy.onLookup(kStrKey);

  ASSERT_EQ(policy.getReuseTime(kStrKey), 2 * kBucketSize);
}
} // namespace facebook::cachelib::navy::tests

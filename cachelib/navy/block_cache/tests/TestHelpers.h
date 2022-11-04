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

#pragma once

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <vector>

#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/FifoPolicy.h"
#include "cachelib/navy/block_cache/Region.h"
#include "cachelib/navy/testing/MockJobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
class MockPolicy : public EvictionPolicy {
 public:
  // NiceMock can only delegate copyable parameters
  explicit MockPolicy(std::vector<uint32_t>* hits) : hits_{*hits} {
    using testing::_;
    using testing::Invoke;
    using testing::Return;

    ON_CALL(*this, track(_)).WillByDefault(Invoke([this](const Region& region) {
      fifo_.track(region);
    }));
    ON_CALL(*this, touch(_)).WillByDefault(Invoke([this](RegionId rid) {
      hits_[rid.index()]++;
      fifo_.touch(rid);
    }));
    ON_CALL(*this, evict()).WillByDefault(Invoke([this] {
      return fifo_.evict();
    }));
    ON_CALL(*this, reset()).WillByDefault(Invoke([this]() {
      std::fill(hits_.begin(), hits_.end(), 0);
      fifo_.reset();
    }));
    ON_CALL(*this, memorySize()).WillByDefault(Invoke([this] {
      return fifo_.memorySize();
    }));
    ON_CALL(*this, getCounters(_))
        .WillByDefault(
            Invoke([this](const CounterVisitor& v) { fifo_.getCounters(v); }));

    ON_CALL(*this, persist(_)).WillByDefault(Invoke([this](RecordWriter& rw) {
      fifo_.persist(rw);
    }));
    ON_CALL(*this, recover(_)).WillByDefault(Invoke([this](RecordReader& rr) {
      fifo_.recover(rr);
    }));
  }

  MOCK_METHOD1(track, void(const Region& region));
  MOCK_METHOD1(touch, void(RegionId rid));
  MOCK_METHOD0(evict, RegionId());
  MOCK_METHOD0(reset, void());
  MOCK_CONST_METHOD0(memorySize, size_t());
  MOCK_CONST_METHOD1(getCounters, void(const CounterVisitor&));

  MOCK_CONST_METHOD1(persist, void(RecordWriter& rw));
  MOCK_METHOD1(recover, void(RecordReader& rr));

 private:
  std::vector<uint32_t>& hits_;
  FifoPolicy fifo_;
};

// @param arg     Region
// @param rid     RegionId
MATCHER_P(EqRegion, rid, "region should have the same id") {
  return arg.id().index() == rid.index();
}

// @param arg     Region
// @param rid     RegionId
// @param pri     Region's priority
MATCHER_P2(EqRegion, rid, pri, "region should have the same id and pri") {
  return arg.id().index() == rid.index() &&
         arg.getPriority() == static_cast<uint32_t>(pri);
}

// @param arg     Region
// @param pri     Region's priority
MATCHER_P(EqRegionPri, pri, "region should have the same pri") {
  return arg.getPriority() == static_cast<uint32_t>(pri);
}

// Return uint32_t of the invalid region id
inline uint32_t getInvalidRegionId() { return RegionId{}.index(); }

// Expect regions specified to be tracked exactly once and in order
inline void expectRegionsTracked(MockPolicy& policy,
                                 std::vector<uint32_t> regionIds,
                                 bool sticky = true) {
  testing::InSequence s;
  for (auto id : regionIds) {
    EXPECT_CALL(policy, track(EqRegion(RegionId{id}))).RetiresOnSaturation();
  }
  if (sticky) {
    EXPECT_CALL(policy, track(testing::_)).Times(0);
  } else {
    EXPECT_CALL(policy, track(testing::_)).Times(testing::AtLeast(0));
  }
}

// Expect regions specified to be touched exactly once and in order
inline void expectRegionsTouched(MockPolicy& policy,
                                 std::vector<uint32_t> regionIds,
                                 bool sticky = true) {
  testing::InSequence s;
  for (auto id : regionIds) {
    EXPECT_CALL(policy, touch(RegionId{id})).RetiresOnSaturation();
  }
  if (sticky) {
    EXPECT_CALL(policy, touch(testing::_)).Times(0);
  } else {
    EXPECT_CALL(policy, touch(testing::_)).Times(testing::AtLeast(0));
  }
}

// Expect regions specified to be evicted exactly once and in order
inline void mockRegionsEvicted(MockPolicy& policy,
                               std::vector<uint32_t> regionIds,
                               bool sticky = true) {
  testing::InSequence s;
  for (auto id : regionIds) {
    EXPECT_CALL(policy, evict())
        .WillOnce(testing::Return(RegionId{id}))
        .RetiresOnSaturation();
  }
  if (sticky) {
    EXPECT_CALL(policy, evict()).Times(0);
  } else {
    EXPECT_CALL(policy, evict()).Times(testing::AtLeast(0));
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

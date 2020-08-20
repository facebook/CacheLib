#pragma once

#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/block_cache/EvictionPolicy.h"
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

    ON_CALL(*this, track(_)).WillByDefault(Return());
    ON_CALL(*this, touch(_)).WillByDefault(Invoke([this](RegionId rid) {
      hits_[rid.index()]++;
    }));
    ON_CALL(*this, reset()).WillByDefault(Invoke([this]() {
      std::fill(hits_.begin(), hits_.end(), 0);
    }));
    ON_CALL(*this, memorySize()).WillByDefault(Return(0));
  }

  MOCK_METHOD1(track, void(RegionId rid));
  MOCK_METHOD1(touch, void(RegionId rid));
  MOCK_METHOD0(evict, RegionId());
  MOCK_METHOD0(reset, void());
  MOCK_CONST_METHOD0(memorySize, size_t());
  MOCK_CONST_METHOD1(getCounters, void(const CounterVisitor&));

 private:
  std::vector<uint32_t>& hits_;
};

// Return uint32_t of the invalid region id
inline uint32_t getInvalidRegionId() { return RegionId{}.index(); }

// Expect regions specified to be tracked exactly once and in order
inline void expectRegionsTrackedInSequence(MockPolicy& policy,
                                           std::vector<uint32_t> regionIds,
                                           bool sticky = true) {
  testing::InSequence s;
  for (auto id : regionIds) {
    EXPECT_CALL(policy, track(RegionId{id})).RetiresOnSaturation();
  }
  if (sticky) {
    EXPECT_CALL(policy, track(testing::_)).Times(0);
  } else {
    EXPECT_CALL(policy, track(testing::_)).Times(testing::AtLeast(0));
  }
}

// Expect regions specified to be touched exactly once and in order
inline void expectRegionsTouchedInSequence(MockPolicy& policy,
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
inline void expectRegionsEvictedInSequence(MockPolicy& policy,
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

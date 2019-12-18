#pragma once

#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cachelib/navy/block_cache/EvictionPolicy.h"

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
} // namespace navy
} // namespace cachelib
} // namespace facebook

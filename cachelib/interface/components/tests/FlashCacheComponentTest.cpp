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

#include <folly/coro/GtestHelpers.h>
#include <gtest/gtest.h>

#include "cachelib/common/Time.h"
#include "cachelib/interface/components/tests/CacheComponentFactory.h"
#include "cachelib/interface/tests/Utils.h"

using namespace facebook::cachelib::interface;
using namespace facebook::cachelib::interface::test;

namespace {

template <typename FactoryType>
class FlashCacheComponentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    factory_ = std::make_unique<FactoryType>();
    cache_ = factory_->create();
    ASSERT_NE(cache_, nullptr) << "Failed to create cache";
  }

  std::unique_ptr<CacheComponent> cache_;

 private:
  std::unique_ptr<FactoryType> factory_;
};

using FlashFactoryTypes =
    ::testing::Types<FlashCacheFactory, ConsistentFlashCacheFactory>;
TYPED_TEST_SUITE(FlashCacheComponentTest, FlashFactoryTypes);

CO_TYPED_TEST(FlashCacheComponentTest, ExpirationCallback) {
  const uint32_t currentTime = facebook::cachelib::util::getCurrentTimeSec();
  constexpr uint32_t kValueSize = 100;
  constexpr uint32_t kTTL = 3600;

  auto valid = ASSERT_OK(co_await this->cache_->allocate(
      "valid_0", kValueSize, currentTime, kTTL));
  EXPECT_OK(co_await this->cache_->insert(std::move(valid)));

  auto valid2 = ASSERT_OK(co_await this->cache_->allocate(
      "valid_1", kValueSize, currentTime - kTTL / 2, kTTL));
  EXPECT_OK(co_await this->cache_->insert(std::move(valid2)));

  auto expired = ASSERT_OK(co_await this->cache_->allocate(
      "expired_0", kValueSize, currentTime - 2 * kTTL, kTTL));
  EXPECT_OK(co_await this->cache_->insert(std::move(expired)));

  auto expired2 = ASSERT_OK(co_await this->cache_->allocate(
      "expired_1", kValueSize, currentTime - 10 * kTTL, kTTL));
  EXPECT_OK(co_await this->cache_->insert(std::move(expired2)));

  // Fill the cache to force region reclamation, which exercises the
  // checkExpired callback configured in FlashCacheComponent::initConfig().
  for (int i = 0; i < 100; ++i) {
    auto res = co_await this->cache_->allocate(
        "fill_" + std::to_string(i), 3000, currentTime, kTTL);
    if (res.hasError()) {
      break;
    }
    EXPECT_OK(co_await this->cache_->insertOrReplace(std::move(res.value())));
  }

  // Expired items should not have been reinserted during reclamation.
  auto rates = this->cache_->getStats().extraStats_.getRates();
  auto statIt = rates.find("navy_bc_evictions_expired");
  CO_ASSERT_NE(statIt, rates.end());
  EXPECT_EQ(statIt->second, 2);

  // Valid items should have been reinserted during reclamation.  Note that
  // reclamation *must* have iterated over these since they were inserted before
  // the expired items.
  for (int i = 0; i < 2; ++i) {
    auto key = "valid_" + std::to_string(i);
    auto findResult = ASSERT_OK(co_await this->cache_->find(key));
    CO_ASSERT_TRUE(findResult.has_value());
    EXPECT_EQ(findResult.value()->getKey(), key);
    EXPECT_EQ(findResult.value()->getExpiryTime(),
              findResult.value()->getCreationTime() + kTTL);
  }
}

} // namespace

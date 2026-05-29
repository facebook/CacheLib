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

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/Time.h"
#include "cachelib/interface/components/tests/CacheComponentFactory.h"
#include "cachelib/interface/tests/Utils.h"

using namespace facebook::cachelib::interface;
using namespace facebook::cachelib::interface::test;

namespace {

class RAMCacheComponentTest : public ::testing::Test {
 protected:
  void SetUp() override {
    factory_ = std::make_unique<RAMCacheFactory>();
    cache_ = factory_->create();
    ASSERT_NE(cache_, nullptr) << "Failed to create cache";
  }

  void TearDown() override { EXPECT_OK(cache_->shutdown()); }

  RAMCacheComponent& ramCache() {
    return static_cast<RAMCacheComponent&>(*cache_);
  }

  void checkNoOutstandingRefs(const std::vector<std::string>& keys) {
    auto& allocator = ramCache().get();
    for (const auto& key : keys) {
      auto implHandle = allocator.find(key);
      ASSERT_TRUE(implHandle) << "Item should still be in cache: " << key;
      EXPECT_EQ(implHandle->getRefCount(), 1u)
          << "Leaked refcount for key: " << key;
    }
  }

  std::unique_ptr<CacheComponent> cache_;

 private:
  std::unique_ptr<RAMCacheFactory> factory_;
};

// ============================================================================
// insertOrReplace() Tests
// ============================================================================

CO_TEST_F(RAMCacheComponentTest, InsertOrReplaceAlwaysReturnsReplacedItem) {
  const std::string key = "replace_key";
  const std::string data1 = "original_data";
  const std::string data2 = "replaced_data";
  const uint32_t now = facebook::cachelib::util::getCurrentTimeSec();

  auto handle1 =
      CO_ASSERT_OK(co_await cache_->allocate(key, data1.size(), now, 3600));
  std::memcpy(handle1->getMemory(), data1.c_str(), data1.size());
  auto result1 =
      CO_ASSERT_OK(co_await cache_->insertOrReplace(std::move(handle1)));
  EXPECT_FALSE(result1.has_value());

  // RAM cache must always return the replaced item
  auto handle2 =
      CO_ASSERT_OK(co_await cache_->allocate(key, data2.size(), now, 3600));
  std::memcpy(handle2->getMemory(), data2.c_str(), data2.size());
  auto result2 =
      CO_ASSERT_OK(co_await cache_->insertOrReplace(std::move(handle2)));
  CO_ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(result2.value()->getKey(), key);
  std::string replacedData(result2.value()->getMemoryAs<const char>(),
                           data1.size());
  EXPECT_EQ(replacedData, data1);
}

// ============================================================================
// iterator() Tests
// ============================================================================

CO_TEST_F(RAMCacheComponentTest, IteratorReleasesRefcounts) {
  const std::vector<std::string> keys = {"iter_ref_1", "iter_ref_2",
                                         "iter_ref_3"};
  const uint32_t now = facebook::cachelib::util::getCurrentTimeSec();

  for (const auto& key : keys) {
    auto handle = CO_ASSERT_OK(co_await cache_->allocate(key, 100, now, 3600));
    EXPECT_OK(co_await cache_->insert(std::move(handle)));
  }

  // Iterate and consume all handles
  {
    auto gen = cache_->iterator();
    while (auto item = co_await gen.next()) {
      // consume and discard
    }
  }

  checkNoOutstandingRefs(keys);
}

CO_TEST_F(RAMCacheComponentTest, ActiveHandleAccounting) {
  const uint32_t now = facebook::cachelib::util::getCurrentTimeSec();
  auto& allocator = ramCache().get();

  constexpr int kNumItems = 3;
  for (int i = 0; i < kNumItems; ++i) {
    auto key = "handle_count_" + std::to_string(i);
    auto handle = CO_ASSERT_OK(co_await cache_->allocate(key, 100, now, 3600));
    EXPECT_OK(co_await cache_->insert(std::move(handle)));
  }

  CO_ASSERT_EQ(allocator.getHandleCountForThread(), 0);

  {
    std::vector<ReadHandle> handles;
    for (int i = 0; i < kNumItems; ++i) {
      auto key = "handle_count_" + std::to_string(i);
      auto result = CO_ASSERT_OK(co_await cache_->find(key));
      CO_ASSERT_TRUE(result.has_value());
      handles.push_back(std::move(result).value());
    }
    EXPECT_EQ(allocator.getHandleCountForThread(), kNumItems);

    for (int i = kNumItems; i > 0; --i) {
      handles.pop_back();
      EXPECT_EQ(allocator.getHandleCountForThread(), i - 1);
    }
  }
}

CO_TEST_F(RAMCacheComponentTest, IteratorEarlyTerminationReleasesRefcounts) {
  constexpr int kNumItems = 10;
  std::vector<std::string> keys;
  const uint32_t now = facebook::cachelib::util::getCurrentTimeSec();

  for (int i = 0; i < kNumItems; ++i) {
    auto& key = keys.emplace_back("early_ref_" + std::to_string(i));
    auto handle = CO_ASSERT_OK(co_await cache_->allocate(key, 100, now, 3600));
    EXPECT_OK(co_await cache_->insert(std::move(handle)));
  }

  // Iterate but break early after 3 items
  {
    auto gen = cache_->iterator();
    int count = 0;
    while (auto item = co_await gen.next()) {
      if (++count >= 3) {
        break;
      }
    }
  }

  checkNoOutstandingRefs(keys);
}

} // namespace

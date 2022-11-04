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

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/CacheTraits.h"
#include "cachelib/allocator/NvmAdmissionPolicy.h"
#include "cachelib/allocator/tests/Cache.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"

namespace facebook {
namespace cachelib {
namespace tests {
class NvmAdmissionPolicyTest : public testing::Test {
 public:
  // Expose the admission policy for testing.
  std::shared_ptr<NvmAdmissionPolicy<CacheAllocator<Cache>>>
  getNvmAdmissionPolicy(CacheAllocator<Cache>& allocator) {
    return allocator.nvmAdmissionPolicy_;
  }

 protected:
  void enableNvmConfig(CacheAllocatorConfig<CacheAllocator<Cache>>& config) {
    CacheAllocator<Cache>::NvmCacheT::Config nvmConfig;
    nvmConfig.navyConfig = utils::getNvmTestConfig("/tmp");
    config.enableNvmCache(nvmConfig);
  }
};

class MockFailedNvmAdmissionPolicy : public NvmAdmissionPolicy<Cache> {
 public:
  using Item = typename Cache::Item;
  using ChainedItemIter = typename Cache::ChainedItemIter;
  MockFailedNvmAdmissionPolicy() = default;

 protected:
  virtual void getCountersImpl(const util::CounterVisitor& v) override {
    v("nvm_mock_failed_policy", 1);
  }

  virtual bool acceptImpl(const Item& it,
                          folly::Range<ChainedItemIter>) override {
    switch (it.getKey().size() % 3) {
    case 0:
      return true;
    case 1:
      return false;
    default:
      throw std::runtime_error("THROW AT THREE");
    }
  }
};

TEST_F(NvmAdmissionPolicyTest, InvalidAPTests) {
  MockFailedNvmAdmissionPolicy ap;
  const Cache::Item item0{"key"};
  folly::Range<Cache::ChainedItemIter> dummyChainedItem;
  // Accept.
  ap.accept(item0, dummyChainedItem);
  auto ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["nvm_mock_failed_policy"], 1);
  EXPECT_EQ(ctrs["ap.called"], 1);
  EXPECT_EQ(ctrs["ap.accepted"], 1);
  EXPECT_EQ(ctrs["ap.rejected"], 0);

  const Cache::Item item1{"key1"};
  // Reject.
  ap.accept(item1, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["nvm_mock_failed_policy"], 1);
  EXPECT_EQ(ctrs["ap.called"], 2);
  EXPECT_EQ(ctrs["ap.accepted"], 1);
  EXPECT_EQ(ctrs["ap.rejected"], 1);

  const Cache::Item item2{"key 2"};
  // Throw.
  EXPECT_THROW(ap.accept(item2, dummyChainedItem), std::runtime_error);
}

// Test the default policy with TTL
TEST_F(NvmAdmissionPolicyTest, MemOnlyTTLTests) {
  NvmAdmissionPolicy<Cache> ap;
  const Cache::Item item0{"key"};
  folly::Range<Cache::ChainedItemIter> dummyChainedItem;

  // No ttl set. Anything gets accepted.
  ap.accept(item0, dummyChainedItem);
  auto ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 1);
  EXPECT_EQ(ctrs["ap.accepted"], 1);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 0);

  const Cache::Item item1{"key1", 10};
  ap.accept(item1, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 2);
  EXPECT_EQ(ctrs["ap.accepted"], 2);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 0);

  // Set TTL to 11.
  ap.initMinTTL(11);
  // The previous 2 items are getting rejected.

  ap.accept(item0, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 3);
  EXPECT_EQ(ctrs["ap.accepted"], 2);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 1);

  ap.accept(item1, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 4);
  EXPECT_EQ(ctrs["ap.accepted"], 2);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 2);

  // Items with TTL of 11 will be accepted
  const Cache::Item item2{"key2", 11};
  ap.accept(item2, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 5);
  EXPECT_EQ(ctrs["ap.accepted"], 3);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 2);

  // Items with TTL >11 will be accepted
  const Cache::Item item3{"key3", 100};
  ap.accept(item3, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["ap.called"], 6);
  EXPECT_EQ(ctrs["ap.accepted"], 4);
  EXPECT_EQ(ctrs["ap.rejected"], 0);
  EXPECT_EQ(ctrs["ap.ttlRejected"], 2);

  // TTL can not be initialized more than once.
  EXPECT_THROW({ ap.initMinTTL(12); }, std::invalid_argument);
}

// Test the initialization of nvm admission policy with TTL.
TEST_F(NvmAdmissionPolicyTest, CacheAllocatorConfigInitTest) {
  using CacheT = CacheAllocator<Cache>;
  using NvmAP = NvmAdmissionPolicy<CacheT>;
  using Config = CacheAllocatorConfig<CacheT>;

  Config config0;
  // Null nvmAP with default ttl (0) will not create a nvmAP.
  CacheAllocator<Cache> cache{config0};
  EXPECT_EQ(this->getNvmAdmissionPolicy(cache), nullptr);

  Config config1;
  this->enableNvmConfig(config1);
  // Null nvmAP with positive ttl creates a nvmAP.
  config1.setNvmAdmissionMinTTL(1);
  CacheT cache1{config1};
  EXPECT_EQ(this->getNvmAdmissionPolicy(cache1)->getMinTTL(), 1);

  // present nvmAP with no ttl initialized.
  Config config2;
  this->enableNvmConfig(config2);
  config2.setNvmCacheAdmissionPolicy(std::make_shared<NvmAP>());

  CacheT cache2{config2};
  EXPECT_EQ(this->getNvmAdmissionPolicy(cache2)->getMinTTL(), 0);

  // validate that the ttl is set correctly regardless of whether it is set
  // before or after the nvm policy.

  // ttl set before policy.
  Config config3;
  this->enableNvmConfig(config3);
  config3.setNvmAdmissionMinTTL(2);
  config3.setNvmCacheAdmissionPolicy(
      std::make_shared<NvmAdmissionPolicy<CacheAllocator<Cache>>>());
  CacheT cache3{config3};
  EXPECT_EQ(this->getNvmAdmissionPolicy(cache3)->getMinTTL(), 2);

  // ttl set after policy.
  Config config4;
  this->enableNvmConfig(config4);
  config4.setNvmCacheAdmissionPolicy(std::make_shared<NvmAP>());
  config4.setNvmAdmissionMinTTL(3);
  CacheT cache4{config4};
  EXPECT_EQ(this->getNvmAdmissionPolicy(cache4)->getMinTTL(), 3);

  // Setting nvm admission min ttl before turning on nvm cache throws.
  Config config5;
  EXPECT_THROW({ config5.setNvmAdmissionMinTTL(5); }, std::invalid_argument);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

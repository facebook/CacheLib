// Copyright 2004-present Facebook. All Rights Reserved.

#include <gtest/gtest.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/allocator/CacheTraits.h"
#include "cachelib/allocator/NvmAdmissionPolicy.h"
#include "cachelib/allocator/tests/NvmTestUtils.h"

namespace facebook {
namespace cachelib {
namespace tests {
// Vanilla version of Cache supplying necessary components to initialize
// CacheAllocatorConfig
struct Cache {
  using AccessType = LruCacheTrait::AccessType;
  using AccessTypeLocks = LruCacheTrait::AccessTypeLocks;
  struct AccessConfig {};
  struct ChainedItemMovingSync {};
  struct RemoveCb {};
  struct NvmCacheFilterCb {};
  struct NvmCacheT {
    struct EncodeCB {};
    struct DecodeCB {};
    struct DeviceEncryptor {};
    struct Config {};
  };
  struct MoveCb {};
  struct Key {};
  struct EventTracker {};
  using MMType = MM2Q;
  struct Item {
    using Key = folly::StringPiece;

    explicit Item(const std::string& key) : key_(key) {}
    Item(const std::string& key, const uint64_t ttl) : key_(key), ttl_(ttl) {}

    Key getKey() const { return key_; }

    std::chrono::seconds getConfiguredTTL() const {
      return std::chrono::seconds(ttl_);
    }

    std::string key_;
    uint64_t ttl_{0};
  };

  using ChainedItemIter = std::vector<Item>::iterator;
};

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
    nvmConfig.dipperOptions = utils::getNvmTestOptions("/tmp");
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
  virtual std::unordered_map<std::string, double> getCountersImpl() override {
    std::unordered_map<std::string, double> ret;
    ret["nvm_mock_failed_policy"] = 1;
    return ret;
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

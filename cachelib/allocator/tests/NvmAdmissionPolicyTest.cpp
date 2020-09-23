// Copyright 2004-present Facebook. All Rights Reserved.

#include "cachelib/allocator/NvmAdmissionPolicy.h"

#include <gtest/gtest.h>

using namespace ::testing;

namespace facebook {
namespace cachelib {
namespace tests {

// Vanilla version of Cache supplying only necessary components:
// Cache::Item
// Cache::Item.getKey()
// Cache::ChainedItemIter
struct Cache {
  struct Item {
    using Key = folly::StringPiece;

    explicit Item(const std::string& key) : key_(key) {}

    Key getKey() const { return key_; }

    std::string key_;
  };

  using ChainedItemIter = std::vector<Item>::iterator;
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

TEST(NvmAdmissionPolicyTests, InvalidAPTests) {
  MockFailedNvmAdmissionPolicy ap;
  const Cache::Item item0{"key"};
  folly::Range<Cache::ChainedItemIter> dummyChainedItem;
  // Accept.
  ap.accept(item0, dummyChainedItem);
  auto ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["nvm_mock_failed_policy"], 1);
  EXPECT_EQ(ctrs["nvm_ap_called"], 1);
  EXPECT_EQ(ctrs["nvm_ap_accepted"], 1);
  EXPECT_EQ(ctrs["nvm_ap_rejected"], 0);

  const Cache::Item item1{"key1"};
  // Reject.
  ap.accept(item1, dummyChainedItem);
  ctrs = ap.getCounters();
  EXPECT_EQ(ctrs["nvm_mock_failed_policy"], 1);
  EXPECT_EQ(ctrs["nvm_ap_called"], 2);
  EXPECT_EQ(ctrs["nvm_ap_accepted"], 1);
  EXPECT_EQ(ctrs["nvm_ap_rejected"], 1);

  const Cache::Item item2{"key 2"};
  // Throw.
  EXPECT_THROW(ap.accept(item2, dummyChainedItem), std::runtime_error);
}
} // namespace tests
} // namespace cachelib
} // namespace facebook

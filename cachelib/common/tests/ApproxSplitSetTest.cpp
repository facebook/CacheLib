#include "cachelib/allocator/memory/tests/TestBase.h"
#include "cachelib/common/ApproxSplitSet.h"

#include <vector>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>


namespace facebook {
namespace cachelib {
namespace tests {

std::string getRandomStr(unsigned int len) {
  return SlabAllocatorTestBase::getRandomStr(len);
}

uint64_t makeHash(const std::string& k) {
  return folly::hash::SpookyHashV2::Hash64(k.data(), k.size(), 0);
}

TEST(ApproxSplitSet, Basic) {
  ApproxSplitSet s{6, 2};
  std::vector<std::string> keys;
  for (int i = 0; i < 6; i++) {
    keys.push_back(getRandomStr(16));
    EXPECT_FALSE(s.insert(makeHash(keys[i])));
  }
  for (int i = 3; i < 6; i++) {
    EXPECT_TRUE(s.insert(makeHash(keys[i])));
  }
  // Add one more. First split is dropped. Because of this adding keys[0]
  // again will not actually add anything (looks like first insert).
  keys.push_back(getRandomStr(16));
  EXPECT_FALSE(s.insert(makeHash(keys[6])));
  EXPECT_FALSE(s.insert(makeHash(keys[0])));
  EXPECT_TRUE(s.insert(makeHash(keys[6])));
}

TEST(ApproxSplitSet, Reset) {
  ApproxSplitSet s{6, 2};
  std::vector<std::string> keys;
  auto printKeys = [&]() {
    std::string s = "\"";
    for (const auto& key : keys) {
      s += std::string{(const char*)key.data(), key.size()};
      s += ", \"";
    }
    return s;
  };

  for (int i = 0; i < 6; i++) {
    keys.push_back(getRandomStr(16));
    EXPECT_FALSE(s.insert(makeHash(keys[i])));
  }

  EXPECT_EQ(6, s.numKeysTracked());

  for (int i = 3; i < 6; i++) {
    EXPECT_TRUE(s.insert(makeHash(keys[i]))) << printKeys();
  }
  EXPECT_EQ(6, s.numKeysTracked());

  s.reset();
  for (int i = 0; i < 6; i++) {
    keys.push_back(getRandomStr(16));
    EXPECT_FALSE(s.insert(makeHash(keys[i])));
  }
  for (int i = 3; i < 6; i++) {
    EXPECT_TRUE(s.insert(makeHash(keys[i])));
  }
}

TEST(ApproxSplitSet, Counters) {
  ApproxSplitSet s{6, 2};

  std::vector<std::string> keys;
  for (int i = 0; i < 6; i++) {
    keys.push_back(getRandomStr(16));
    EXPECT_FALSE(s.insert(makeHash(keys[i])));
  }

  EXPECT_EQ(keys.size(), s.numKeysTracked());

  for (int i = 3; i < 6; i++) {
    EXPECT_TRUE(s.insert(makeHash(keys[i])));
  }

  EXPECT_EQ(keys.size(), s.numKeysTracked());

  // Add one more. First split is dropped. Because of this adding keys[0]
  // again will not actually add anything (looks like first insert).
  keys.push_back(getRandomStr(16));
  EXPECT_FALSE(s.insert(makeHash(keys[6])));
  EXPECT_FALSE(s.insert(makeHash(keys[0])));
  EXPECT_TRUE(s.insert(makeHash(keys[6])));

  // each set is 3 keys and the first split got dropped and we add two new
  // keys
  EXPECT_EQ(5, s.numKeysTracked());

  std::this_thread::sleep_for(std::chrono::seconds(5));
  EXPECT_GE(5, s.trackingWindowDurationSecs());

  s.reset();
  EXPECT_EQ(0, s.numKeysTracked());
  EXPECT_GE(1, s.trackingWindowDurationSecs());
}

TEST(RejectFirstAP, Collision) {
  std::array<std::string, 6> keysOneCollision = {
      "J725x0pGo27DqA05", "Sj4HwHdC9t2jmpst", "AyXl5cuM6HUEuhrG",
      "V0i022x65n19bbMI", "6ftusy5yO4Qwr8x7", "EnuKP9e25Tw5O8cO"};
  std::array<std::string, 6> keysTwoCollision = {
      "d8w3L8xsZH4Fr7Xt", "Ml49bGHL0X3pkL66", "j58Sqk7dhDCiquGe",
      "ou4496K75V429UBc", "0m0Jpdj0B1OP8Q02", "0t8I7Ftd8n00UMT8"};
  auto doTest = [](const std::array<std::string, 6>& keys) {
    ApproxSplitSet s{6, 2};
    for (const auto& k : keys) {
      EXPECT_FALSE(s.insert(makeHash(k)));
    }

    EXPECT_EQ(keys.size(), s.numKeysTracked());

    // given we have inserted 6 keys and key count matches with collision,
    // accept should all return true.
    for (const auto& k : keys) {
      EXPECT_TRUE(s.insert(makeHash(k))) << k;
    }
  };
  doTest(keysOneCollision);
  doTest(keysTwoCollision);
}
} // namespace tests
} // namespace cachelib
} // namespace facebook

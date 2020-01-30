#include <limits>
#include <string>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/common/Hash.h"

namespace facebook {
namespace cachelib {

static const int kMaxKeySize = 2048;
static std::mt19937 gen(folly::Random::rand32());

std::string generateRandomString() {
  static std::uniform_int_distribution<> genChar(0, 255);
  std::string ret;
  for (int i = 0; i < kMaxKeySize; i++) {
    ret.push_back(genChar(gen));
  }
  return ret;
}

TEST(Hash, Murmur2) {
  int numTests = 10000;
  facebook::cachelib::MurmurHash2 murmur;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = murmur(reinterpret_cast<void*>(str.data()), str.size());
    EXPECT_EQ(hash, murmur(reinterpret_cast<void*>(str.data()), str.size()));
  }
}

TEST(Hash, Murmur2Other) {
  int numTests = 10000;
  facebook::cachelib::MurmurHash2 murmur;
  facebook::cachelib::MurmurHash2 murmurOther;

  for (int i = 0; i < numTests; i++) {
    auto str = generateRandomString();
    uint32_t hash = murmur(reinterpret_cast<void*>(str.data()), str.size());
    EXPECT_EQ(hash,
              murmurOther(reinterpret_cast<void*>(str.data()), str.size()));
  }
}

} // namespace cachelib
} // namespace facebook

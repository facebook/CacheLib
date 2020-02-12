#include <random>

#include <folly/Random.h>
#include <gtest/gtest.h>

#include "cachelib/common/CountMinSketch.h"

namespace facebook {
namespace cachelib {
namespace tests {
using facebook::cachelib::util::CountMinSketch;

TEST(CountMinSketch, Simple) {
  CountMinSketch cms{100, 3};
  std::mt19937_64 rg{1};
  std::vector<uint64_t> keys;
  for (uint32_t i = 0; i < 10; i++) {
    keys.push_back(rg());
    for (uint32_t j = 0; j < i; j++) {
      cms.increment(keys[i]);
    }
  }

  for (uint32_t i = 0; i < keys.size(); i++) {
    EXPECT_GE(cms.getCount(keys[i]), i);
  }
}

TEST(CountMinSketch, Remove) {
  CountMinSketch cms{100, 3};
  std::mt19937_64 rg{1};
  std::vector<uint64_t> keys;
  for (uint32_t i = 0; i < 10; i++) {
    keys.push_back(rg());
    for (uint32_t j = 0; j < i; j++) {
      cms.increment(keys[i]);
    }
  }

  for (uint32_t i = 0; i < keys.size(); i++) {
    cms.resetCount(keys[i]);
    EXPECT_EQ(0, cms.getCount(keys[i]));
  }
}

TEST(CountMinSketch, Reset) {
  CountMinSketch cms{100, 3};
  std::mt19937_64 rg{1};
  std::vector<uint64_t> keys;
  for (uint32_t i = 0; i < 10; i++) {
    keys.push_back(rg());
    cms.increment(keys[i]);
  }

  cms.reset();
  for (uint32_t i = 0; i < keys.size(); i++) {
    EXPECT_EQ(0, cms.getCount(keys[i]));
  }
}

TEST(CountMinSketch, Collisions) {
  // Equivalent to 5% error with 99% certainty
  CountMinSketch cms{40, 5};
  std::mt19937_64 rg{1};
  std::vector<uint64_t> keys;
  uint32_t sum{0};
  // Try inserting more keys than cms table width
  for (uint32_t i = 0; i < 55; i++) {
    keys.push_back(rg());
    for (uint32_t j = 0; j < i; j++) {
      cms.increment(keys[i]);
    }
    sum += i;
  }

  auto errorMargin = sum * 0.05;
  for (uint32_t i = 0; i < keys.size(); i++) {
    // Expect all counts are within error margin
    EXPECT_GE(cms.getCount(keys[i]), i);
    EXPECT_GE(i + errorMargin, cms.getCount(keys[i]));
  }
}

TEST(CountMinSketch, ProbabilityConstructor) {
  CountMinSketch cms{0.01, 0.95, 0, 0};
  EXPECT_EQ(200, cms.width());
  EXPECT_EQ(5, cms.depth());
}

TEST(CountMinSketch, InvalidArgs) {
  EXPECT_THROW(CountMinSketch(0, 0, 0, 0), std::invalid_argument);
}

// ensure all the apis return menaningful results on a default constructed
// empty object.
TEST(CountMinSketch, Default) {
  CountMinSketch cms{};
  uint64_t key = folly::Random::rand32();
  EXPECT_EQ(cms.getCount(key), 0);
  EXPECT_NO_THROW(cms.increment(key));
  EXPECT_EQ(cms.getCount(key), 0);
  EXPECT_EQ(0, cms.getByteSize());
  cms.decayCountsBy(0.1);
  EXPECT_EQ(0, cms.getCount(key));
}

TEST(CountMinSketch, Move) {
  CountMinSketch cms{40, 5};
  uint64_t key = folly::Random::rand32();
  int cnt = 20;
  for (int i = 0; i < cnt; i++) {
    cms.increment(key);
  }

  EXPECT_GE(cnt, cms.getCount(key));

  auto cms2 = std::move(cms);
  EXPECT_GE(cnt, cms2.getCount(key));
  EXPECT_EQ(0, cms.getCount(key));
}

// populate counts and ensure that decay has effect on getCounts
TEST(CountMinSketch, DecayCounts) {
  CountMinSketch cms{100, 3};
  std::mt19937_64 rg{1};
  std::vector<uint64_t> keys;
  // key i is incremented i times.
  for (uint32_t i = 0; i < 1000; i++) {
    keys.push_back(rg());
    for (uint32_t j = 0; j < i; j++) {
      cms.increment(keys[i]);
    }
  }

  for (uint32_t i = 0; i < keys.size(); i++) {
    EXPECT_GE(cms.getCount(keys[i]), i);
  }

  const double decayFactor = 0.5;
  cms.decayCountsBy(decayFactor);

  for (uint32_t i = 0; i < keys.size(); i++) {
    EXPECT_GE(cms.getCount(keys[i]), i * decayFactor);
  }
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

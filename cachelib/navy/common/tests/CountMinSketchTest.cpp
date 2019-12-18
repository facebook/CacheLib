#include "cachelib/navy/common/CountMinSketch.h"

#include <random>

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
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
    EXPECT_GE(i, cms.getCount(keys[i]));
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
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

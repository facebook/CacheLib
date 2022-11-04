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

#include <folly/Random.h>
#include <gtest/gtest.h>

#include <random>

#include "cachelib/common/CountMinSketch.h"

namespace facebook {
namespace cachelib {
namespace tests {
using facebook::cachelib::util::CountMinSketch;
using facebook::cachelib::util::CountMinSketch16;
using facebook::cachelib::util::CountMinSketch8;
using facebook::cachelib::util::detail::CountMinSketchBase;

template <typename UINT, typename CT>
UINT sanitizeCt(CT ct, CountMinSketchBase<UINT>& cms) {
  if (ct > cms.getMaxCount()) {
    return cms.getMaxCount();
  } else {
    return static_cast<UINT>(ct);
  }
}

template <typename CMS>
class CountMinSketchTest : public testing::Test {
 protected:
  void testSimple() {
    CMS cms{100, 3};
    std::mt19937_64 rg{1};
    std::vector<uint64_t> keys;
    for (uint32_t i = 0; i < 10; i++) {
      keys.push_back(rg());
      for (uint32_t j = 0; j < i; j++) {
        cms.increment(keys[i]);
      }
    }

    for (uint32_t i = 0; i < keys.size(); i++) {
      EXPECT_GE(cms.getCount(keys[i]), sanitizeCt(i, cms));
    }
  }

  void testRemove() {
    CMS cms{100, 3};
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

  void testReset() {
    CMS cms{100, 3};
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

  void testCollisions() {
    CMS cms{40, 5};
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

  void testProbabilityConstructor() {
    CMS cms{0.01, 0.95, 0, 0};
    EXPECT_EQ(200, cms.width());
    EXPECT_EQ(5, cms.depth());
  }

  void testInvalidArgs() {
    EXPECT_THROW(CMS(0, 0, 0, 0), std::invalid_argument);
  }

  void testDefault() {
    CMS cms{};
    uint64_t key = folly::Random::rand32();
    EXPECT_EQ(cms.getCount(key), 0);
    EXPECT_NO_THROW(cms.increment(key));
    EXPECT_EQ(cms.getCount(key), 0);
    EXPECT_EQ(0, cms.getByteSize());
    cms.decayCountsBy(0.1);
    EXPECT_EQ(0, cms.getCount(key));
  }

  void testMove() {
    CMS cms{40, 5};
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
  void testDecayCounts() {
    CMS cms{100, 3};
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
      EXPECT_GE(cms.getCount(keys[i]), sanitizeCt(i, cms));
    }

    const double decayFactor = 0.5;
    cms.decayCountsBy(decayFactor);

    for (uint32_t i = 0; i < keys.size(); i++) {
      EXPECT_GE(cms.getCount(keys[i]),
                // Floor to int for a fair comparison. Otherwise may we may be
                // expecting 127 >= 127.5 for uint8_t
                std::floor(sanitizeCt(i, cms) * decayFactor));
    }
  }

  void testOverflow() {
    CMS cms{10, 3};
    uint64_t key = folly::Random::rand32();
    uint64_t max = static_cast<uint64_t>(cms.getMaxCount()) + 2;
    // Skip the test for large count type since it times out the unit test.
    if (max < std::numeric_limits<uint32_t>::max()) {
      for (uint64_t i = 0; i < max; i++) {
        cms.increment(key);
      }

      ASSERT_EQ(cms.getCount(key), sanitizeCt(max, cms));
      ASSERT_EQ(cms.getSaturatedCounts(), 3);
    }
  }
};
typedef ::testing::Types<CountMinSketch, CountMinSketch16, CountMinSketch8>
    CMSTypes;

TYPED_TEST_CASE(CountMinSketchTest, CMSTypes);

TYPED_TEST(CountMinSketchTest, Simple) { this->testSimple(); }

TYPED_TEST(CountMinSketchTest, Remove) { this->testRemove(); }

TYPED_TEST(CountMinSketchTest, Reset) { this->testReset(); }

TYPED_TEST(CountMinSketchTest, Collisions) { this->testCollisions(); }

TYPED_TEST(CountMinSketchTest, ProbabilityConstructor) {
  this->testProbabilityConstructor();
}

TYPED_TEST(CountMinSketchTest, InvalidArgs) { this->testInvalidArgs(); }

// ensure all the apis return menaningful results on a default constructed
// empty object.
TYPED_TEST(CountMinSketchTest, Default) { this->testDefault(); }

TYPED_TEST(CountMinSketchTest, Move) { this->testMove(); }

TYPED_TEST(CountMinSketchTest, DecayCounts) { this->testDefault(); }

// Make sure we don't crash when the count overflows.
TYPED_TEST(CountMinSketchTest, Overflow) { this->testOverflow(); }

} // namespace tests
} // namespace cachelib
} // namespace facebook

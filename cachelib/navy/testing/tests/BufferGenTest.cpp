#include "cachelib/navy/testing/BufferGen.h"

#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
namespace {
bool isRange(uint8_t b, uint8_t rmin, uint8_t rmax) {
  return rmin <= b && b <= rmax;
}

bool isAlphaChar(uint8_t b) {
  return isRange(b, 'a', 'z') || isRange(b, 'A', 'Z') || isRange(b, '0', '9') ||
         b == '+' || b == '=';
}
} // namespace

TEST(BufferGen, Basic) {
  BufferGen bg{1};
  uint32_t count[10]{};
  for (uint32_t i = 0; i < 100; i++) {
    auto buf = bg.gen(4, 8);
    EXPECT_LE(4, buf.size());
    EXPECT_GT(8, buf.size());
    count[buf.size()]++;
    for (size_t j = 0; j < buf.size(); j++) {
      EXPECT_TRUE(isAlphaChar(buf.data()[j]));
    }
  }
  EXPECT_LT(0, count[4]);
  EXPECT_LT(0, count[5]);
  EXPECT_LT(0, count[6]);
  EXPECT_LT(0, count[7]);
}

TEST(BufferGen, Seed) {
  BufferGen bg1{1};
  auto buf1 = bg1.gen(10);
  BufferGen bg2{1};
  auto buf2 = bg2.gen(10);
  EXPECT_EQ(buf1.view(), buf2.view());
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

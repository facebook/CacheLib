#include "cachelib/cachebench/consistency/RingBuffer.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
TEST(RingBuffer, Overflow) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.size());
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.write(11));
  EXPECT_EQ(2, rb.write(12));
  EXPECT_EQ(3, rb.size());
  EXPECT_THROW(rb.write(13), std::runtime_error);
}

// Test read/write and buffer wrap
TEST(RingBuffer, Underflow) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.size());
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.size());
  EXPECT_EQ(10, rb.read());
  EXPECT_EQ(0, rb.size());
  EXPECT_THROW(rb.read(), std::runtime_error);
}

TEST(RingBuffer, ReadRewrite) {
  RingBuffer<uint32_t, 3> rb;
  EXPECT_EQ(0, rb.write(10));
  EXPECT_EQ(1, rb.write(11));
  EXPECT_EQ(2, rb.write(12));

  EXPECT_EQ(10, rb.getAt(0));
  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));

  EXPECT_EQ(10, rb.read());

  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));
  EXPECT_THROW(rb.getAt(0), std::out_of_range);
  EXPECT_THROW(rb.getAt(3), std::out_of_range);

  EXPECT_EQ(3, rb.write(13));
  EXPECT_EQ(11, rb.getAt(1));
  EXPECT_EQ(12, rb.getAt(2));
  EXPECT_EQ(13, rb.getAt(3));
  rb.setAt(3, 14);
  EXPECT_EQ(14, rb.getAt(3));

  EXPECT_EQ(3, rb.size());
  EXPECT_EQ(11, rb.read());
  EXPECT_EQ(12, rb.read());
  EXPECT_EQ(14, rb.read());
  EXPECT_EQ(0, rb.size());
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

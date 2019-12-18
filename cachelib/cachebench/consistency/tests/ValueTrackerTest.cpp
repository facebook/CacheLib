#include "cachelib/cachebench/consistency/ValueTracker.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
// Test interleaving key events
TEST(ValueTracker, Basic) {
  std::vector<std::string> ss{"cat", "dog"};
  ValueTracker vt{ValueTracker::wrapStrings(ss)};
  auto catSet = vt.beginSet("cat", 0);
  vt.endSet(catSet);
  auto catGet = vt.beginGet("cat");
  vt.beginSet("dog", 0); // Set without end
  auto dogGet = vt.beginGet("dog");
  EXPECT_TRUE(vt.endGet(catGet, 0, true));
  EXPECT_FALSE(vt.endGet(dogGet, 1, true)); // Inconsistent
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

#include "cachelib/navy/admission_policy/RejectRandomAP.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace tests {
TEST(RejectRandomAP, Basic) {
  RejectRandomAP::Config config;
  config.probability = 0.9;
  RejectRandomAP ap{std::move(config)};
  uint32_t rejected = 0;
  uint32_t accepted = 0;
  for (int i = 0; i < 100; i++) {
    if (ap.accept(makeHK("key"), makeView("value"))) {
      accepted++;
    } else {
      rejected++;
    }
  }
  EXPECT_GT(85, rejected);
  EXPECT_LT(15, accepted);
}

TEST(RejectRandomAP, Prob1) {
  RejectRandomAP::Config config;
  config.probability = 1.0;
  RejectRandomAP ap{std::move(config)};
  for (int i = 0; i < 100; i++) {
    EXPECT_TRUE(ap.accept(makeHK("key"), makeView("value")));
  }
}
} // namespace tests
} // namespace navy
} // namespace cachelib
} // namespace facebook

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {
struct WrittenBytes {
  uint64_t bytes;
};
} // namespace

TEST(DynamicRandomAPTest, AboveTarget) {
  DynamicRandomAP::Config config;
  config.targetRate = 1;
  config.updateInterval = std::chrono::seconds{1};
  config.probabilitySeed = 0.5;
  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  DynamicRandomAP ap{std::move(config)};

  std::string item{"keyvalue"};
  int accepted{0}, rejected{0};
  for (int i = 0; i < 100; i++) {
    if (ap.accept(makeHK("key"), makeView("value"))) {
      accepted++;
      bytesWritten += 8;
    } else {
      rejected++;
    }
  }

  // Force admission policy to update probability factor
  ap.update();

  int acceptedNew{0}, rejectedNew{0};
  for (int i = 0; i < 100; i++) {
    if (ap.accept(makeHK("key"), makeView("value"))) {
      acceptedNew++;
      bytesWritten += 8;
    } else {
      rejectedNew++;
    }
  }

  // Probability factor should have decreased since more bytes were accepted
  // than desired
  EXPECT_LE(rejected, rejectedNew);
  EXPECT_GE(accepted, acceptedNew);
}

TEST(DynamicRandomAPTest, BelowTarget) {
  DynamicRandomAP::Config config;
  config.targetRate = 1000;
  config.updateInterval = std::chrono::seconds{1};
  config.probabilitySeed = 0.5;
  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  DynamicRandomAP ap{std::move(config)};

  std::string item{"keyvalue"};
  int accepted{0}, rejected{0};
  for (int i = 0; i < 100; i++) {
    if (ap.accept(makeHK("key"), makeView("value"))) {
      accepted++;
      bytesWritten += 8;
    } else {
      rejected++;
    }
  }

  // Force admission policy to update probability factor
  ap.update();

  int acceptedNew{0}, rejectedNew{0};
  for (int i = 0; i < 100; i++) {
    if (ap.accept(makeHK("key"), makeView("value"))) {
      acceptedNew++;
      bytesWritten += 8;
    } else {
      rejectedNew++;
    }
  }

  // Probability factor should have increased since more bytes were accepted
  // than desired
  EXPECT_GE(rejected, rejectedNew);
  EXPECT_LE(accepted, acceptedNew);
}

TEST(DynamicRandomAPTest, BelowTargetSuffix) {
  DynamicRandomAP::Config config;
  config.targetRate = 1000;
  config.updateInterval = std::chrono::seconds{1};
  config.probabilitySeed = 0.5;
  config.deterministicKeyHashSuffixLength = 2;
  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  DynamicRandomAP ap{std::move(config)};

  int accepted{0}, rejected{0};
  std::string acceptKey = "key ";
  std::string rejectKey = "rey ";
  int times = 100;
  for (int i = 0; i < times; i++) {
    acceptKey[3] = static_cast<unsigned char>(i + 1);
    if (ap.accept(makeHK(acceptKey.c_str()), makeView("value"))) {
      accepted++;
      bytesWritten += 8;
    } else {
      rejected++;
    }
  }
  // should be deterministically accepted
  EXPECT_EQ(rejected, 0);
  EXPECT_EQ(accepted, times);

  rejected = 0;
  accepted = 0;

  for (int i = 0; i < times; i++) {
    rejectKey[3] = static_cast<unsigned char>(i + 1);
    if (ap.accept(makeHK(rejectKey.c_str()), makeView("value"))) {
      accepted++;
      bytesWritten += 8;
    } else {
      rejected++;
    }
  }

  EXPECT_EQ(rejected, times);
  EXPECT_EQ(accepted, 0);

  // should hash the whole key
  EXPECT_TRUE(ap.accept(makeHK("k"), makeView("value")));
  EXPECT_FALSE(ap.accept(makeHK("\xbc"), makeView("value")));
}

TEST(DynamicRandomAPTest, BadConfig) {
  DynamicRandomAP::Config config;
  config.targetRate = 1000;
  config.updateInterval = std::chrono::seconds{1};
  config.probabilitySeed = 0;
  EXPECT_THROW(DynamicRandomAP(std::move(config)), std::invalid_argument);
}

// Make sure that the probabilityFactor always stay in the range.
TEST(DynamicRandomAPTest, StayInRange) {
  WrittenBytes wb;
  DynamicRandomAP::Config config;
  config.targetRate = 34 * 1024 * 1024;
  config.updateInterval = std::chrono::seconds{60};
  std::chrono::seconds time{getSteadyClockSeconds()};

  config.fnBytesWritten = [&] { return wb.bytes; };
  auto ap = facebook::cachelib::navy::DynamicRandomAP(std::move(config));
  auto checkInRange = [](DynamicRandomAP& ap) {
    auto params = ap.getThrottleParams();
    ASSERT_LE(params.probabilityFactor, ap.kUpperBound_);
    ASSERT_GE(params.probabilityFactor, ap.kLowerBound_);
  };

  for (size_t i = 0; i < 50000; i++) {
    wb.bytes += 1 * 1024 * 1024 * config.updateInterval.count();
    time = time + config.updateInterval;
    ap.updateThrottleParams(time);
    checkInRange(ap);
  }

  for (size_t i = 0; i < 50000; i++) {
    wb.bytes += 1000 * 1024 * 1024 * config.updateInterval.count();
    time = time + config.updateInterval;
    ap.updateThrottleParams(time);
    checkInRange(ap);
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

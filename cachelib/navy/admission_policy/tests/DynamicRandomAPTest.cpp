#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

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
  DynamicRandomAP::Config config;
  config.targetRate = 34 * 1024 * 1024;
  config.updateInterval = std::chrono::seconds{60};
  std::chrono::seconds time{getSteadyClockSeconds()};

  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  auto ap = facebook::cachelib::navy::DynamicRandomAP(std::move(config));
  auto checkInRange = [](DynamicRandomAP& ap) {
    auto params = ap.getThrottleParams();
    ASSERT_LE(params.probabilityFactor, ap.kUpperBound_);
    ASSERT_GE(params.probabilityFactor, ap.kLowerBound_);
  };

  for (size_t i = 0; i < 50000; i++) {
    bytesWritten += 1 * 1024 * 1024 * config.updateInterval.count();
    time = time + config.updateInterval;
    ap.updateThrottleParams(time);
    checkInRange(ap);
  }

  for (size_t i = 0; i < 50000; i++) {
    bytesWritten += 1000 * 1024 * 1024 * config.updateInterval.count();
    time = time + config.updateInterval;
    ap.updateThrottleParams(time);
    checkInRange(ap);
  }
}

// Make sure that the probabilityFactor is adjusted towards the max cap instead
// of the current write rate.
TEST(DynamicRandomAPTest, RespectMaxWriteRate) {
  DynamicRandomAP::Config config;
  config.targetRate = 70 * 1024 * 1024;
  config.maxRate = 80 * 1024 * 1024;
  config.updateInterval = std::chrono::seconds{36000};
  std::chrono::seconds time{getSteadyClockSeconds()};

  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  auto ap = facebook::cachelib::navy::DynamicRandomAP(std::move(config));

  // Write 36000 byte in 10 hours.
  bytesWritten = 36000;
  time = time + config.updateInterval;
  ap.updateThrottleParams(time);
  // Observed write rate 1
  // Untrimmed current target write rate should be 98.75MB/s, but the
  // probabilityFactor should adjust towards 80MB/s
  // rawFactor should be 1.14 instead of 1.41
  // Since the factor shouldn't be clamped, we can check the factor is in range.
  auto params = ap.getThrottleParams();
  ASSERT_LE(params.probabilityFactor * params.observedCurRate_, config.maxRate);
  ASSERT_EQ(params.curTargetRate, config.maxRate);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

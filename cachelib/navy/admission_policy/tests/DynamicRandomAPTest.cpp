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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace {

// Create a DynamicRandomAP with given target rate that
// accepts all items before the first update and treats odd lenth key as
// bypassed.
navy::DynamicRandomAP createTestBypassAP(uint64_t& bytesWritten,
                                         uint64_t targetRate) {
  navy::DynamicRandomAP::Config config;
  config.targetRate = targetRate;
  config.maxRate = 80 * 1024 * 1024;
  config.updateInterval = std::chrono::seconds{10};
  config.probabilitySeed = 1.0; // Always accept;
  // Allow a large change factor so that we can assert probFactor change.
  config.changeWindow = 0.999;
  // Even sized keys are primary.
  config.fnBypass = [](folly::StringPiece key) { return key.size() % 2 == 1; };

  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  return facebook::cachelib::navy::DynamicRandomAP(std::move(config));
}
} // namespace

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
    ap.updateThrottleParamsLocked(time);
    checkInRange(ap);
  }

  for (size_t i = 0; i < 50000; i++) {
    bytesWritten += 1000 * 1024 * 1024 * config.updateInterval.count();
    time = time + config.updateInterval;
    ap.updateThrottleParamsLocked(time);
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
  ap.updateThrottleParamsLocked(time);
  // Observed write rate 1
  // Untrimmed current target write rate should be 98.75MB/s, but the
  // probabilityFactor should adjust towards 80MB/s
  // rawFactor should be 1.14 instead of 1.41
  // Since the factor shouldn't be clamped, we can check the factor is in range.
  auto params = ap.getThrottleParams();
  auto writeStats = ap.getWriteStats();
  ASSERT_LE(params.probabilityFactor * writeStats.observedCurRate,
            config.maxRate);
  ASSERT_EQ(writeStats.curTargetRate, config.maxRate);
}

// Being able to count accepted rate and observed rate differently.
TEST(DynamicRandomAPTest, AcceptedBytesCount) {
  DynamicRandomAP::Config config;
  config.targetRate = 70 * 1024 * 1024;
  config.maxRate = 80 * 1024 * 1024;
  config.updateInterval = std::chrono::seconds{10};
  config.probabilitySeed = 1.0; // Always accept;
  std::chrono::seconds time{getSteadyClockSeconds()};
  uint64_t acceptedBytes;

  uint64_t bytesWritten{0};
  config.fnBytesWritten = [&bytesWritten]() { return bytesWritten; };
  // Simulate 10 seconds later.
  time = time + config.updateInterval;
  auto ap = facebook::cachelib::navy::DynamicRandomAP(std::move(config));

  // Observe 36000 byte in 10 seconds.
  bytesWritten = 36000;
  auto key = makeHK("key");
  auto val = makeView("valueXXXXXXXXXXXXXXX");
  ap.accept(key, val);
  acceptedBytes = key.key().size() + val.size();
  ap.updateThrottleParamsLocked(time);
  auto stats = ap.getWriteStats();
  // No accepted bytes.
  ASSERT_EQ(stats.observedCurRate,
            bytesWritten / config.updateInterval.count());
  ASSERT_EQ(stats.acceptedRate, acceptedBytes / config.updateInterval.count());
}

// Only throttle primary.
TEST(DynamicRandomAPTest, ThrottleRegular) {
  uint64_t bytesWritten{0};
  std::chrono::seconds time{getSteadyClockSeconds()};
  auto ap = createTestBypassAP(bytesWritten, 1000);

  // Observe 36000 byte in 10 seconds. Total observed rate 3600B/s
  bytesWritten = 36000;
  time = time + ap.updateInterval_;

  // Setup regular and bypassed items so that they have the same size.
  // Regular item.
  auto priKey = makeHK("key0");
  auto priVal = makeView("valueXXXXXXXXXXXXXXX");
  // Bypassed item.
  auto secKey = makeHK("key");
  auto secVal = makeView("valueXXXXXXXXXXXXXXXX");

  // regular observed rate: 2700B/s
  // bypassed rate: 900B/s
  ap.accept(priKey, priVal);
  ap.accept(priKey, priVal);
  ap.accept(priKey, priVal);
  ap.accept(secKey, secVal);

  ap.updateThrottleParamsLocked(time);
  auto params = ap.getThrottleParams();
  auto stats = ap.getWriteStats();

  // Throttled towards 99B/s from 2700B/s
  // (Not 100B/s because we overwritten in the first time)
  // Target rate is 99 + 900
  ASSERT_EQ(stats.curTargetRate, 999);
  ASSERT_EQ(params.probabilityFactor, 99.0 / 2700);
}

// Throttle both (primary throttled to 0)
TEST(DynamicRandomAPTest, ThrottleAll) {
  std::chrono::seconds time{getSteadyClockSeconds()};
  uint64_t bytesWritten{0};
  auto ap = createTestBypassAP(bytesWritten, 1000);

  // Observe 36000 byte in 10 seconds. Total observed rate 3600B/s
  bytesWritten = 36000;
  time = time + ap.updateInterval_;

  // Setup primary and secondary items so that they have the same size.
  // Primary item.
  auto priKey = makeHK("key0");
  auto priVal = makeView("valueXXXXXXXXXXXXXXX");
  // Secondary item.
  auto secKey = makeHK("key");
  auto secVal = makeView("valueXXXXXXXXXXXXXXXX");

  // primary observed rate: 1800B/s
  // secondary observed rate: 1800B/s
  ap.accept(priKey, priVal);
  ap.accept(priKey, priVal);
  ap.accept(secKey, secVal);
  ap.accept(secKey, secVal);

  ap.updateThrottleParamsLocked(time);
  auto params = ap.getThrottleParams();
  auto stats = ap.getWriteStats();

  // Regular is throttled towards 0
  // Overall target rate is 999, all from bypass traffic.
  ASSERT_EQ(stats.curTargetRate, 999);
  ASSERT_EQ(params.probabilityFactor, ap.minChange_);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

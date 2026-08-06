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

#include <gtest/gtest.h>

#include "cachelib/cachebench/util/Config.h"

namespace facebook::cachelib::cachebench {
namespace {

TEST(StressorConfigTest, DramIteratorDefaultsDisabled) {
  const StressorConfig config{folly::dynamic::object()};

  EXPECT_EQ(DramIteratorMode::kDisabled, config.dramIteratorMode);
  EXPECT_EQ(10000, config.dramIteratorIntervalMs);
  EXPECT_EQ(0, config.dramIteratorSleepMs);
  EXPECT_EQ(0, config.dramIteratorWorkMs);
  EXPECT_FALSE(config.usesDramIterator());
  EXPECT_FALSE(config.usesRegularDramIterator());
  EXPECT_FALSE(config.usesLockGroupDramIterator());
}

TEST(StressorConfigTest, ParsesRegularDramIteratorMode) {
  folly::dynamic configJson = folly::dynamic::object(
      "dramIteratorMode", "regular")("dramIteratorIntervalMs", 25)(
      "dramIteratorSleepMs", 3)("dramIteratorWorkMs", 7);

  const StressorConfig config{configJson};

  EXPECT_EQ(DramIteratorMode::kRegular, config.dramIteratorMode);
  EXPECT_TRUE(config.usesDramIterator());
  EXPECT_TRUE(config.usesRegularDramIterator());
  EXPECT_FALSE(config.usesLockGroupDramIterator());
  EXPECT_EQ(25, config.dramIteratorIntervalMs);
  EXPECT_EQ(3, config.dramIteratorSleepMs);
  EXPECT_EQ(7, config.dramIteratorWorkMs);
}

TEST(StressorConfigTest, ParsesLockGroupDramIteratorMode) {
  const StressorConfig config{
      folly::dynamic::object("dramIteratorMode", "lock_group")};

  EXPECT_EQ(DramIteratorMode::kLockGroup, config.dramIteratorMode);
  EXPECT_TRUE(config.usesDramIterator());
  EXPECT_FALSE(config.usesRegularDramIterator());
  EXPECT_TRUE(config.usesLockGroupDramIterator());
}

TEST(StressorConfigTest, RejectsInvalidDramIteratorMode) {
  EXPECT_THROW(StressorConfig{folly::dynamic::object("dramIteratorMode",
                                                     "not_an_iterator")},
               std::invalid_argument);
}

TEST(StressorConfigTest, RejectsZeroDramIteratorIntervalWhenEnabled) {
  EXPECT_THROW(StressorConfig{folly::dynamic::object(
                   "dramIteratorMode", "regular")("dramIteratorIntervalMs", 0)},
               std::invalid_argument);
}

TEST(StressorConfigTest, RejectsNegativeDramIteratorThrottleValues) {
  EXPECT_THROW(
      StressorConfig{folly::dynamic::object("dramIteratorSleepMs", -1)},
      std::invalid_argument);
  EXPECT_THROW(StressorConfig{folly::dynamic::object("dramIteratorWorkMs", -1)},
               std::invalid_argument);
}

TEST(StressorConfigTest, RejectsDramIteratorWorkWithoutSleep) {
  EXPECT_THROW(StressorConfig{folly::dynamic::object("dramIteratorWorkMs", 1)},
               std::invalid_argument);
}

} // namespace
} // namespace facebook::cachelib::cachebench

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

#include "cachelib/navy/admission_policy/RejectRandomAP.h"

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

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

#include <cctype>

#include "cachelib/cachebench/consistency/ValueHistory.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace tests {
TEST(EventInfo, FormatTime) {
  EventInfo ei;
  ei.tp = EventInfo::TimePoint{
      EventInfo::TimePoint::duration(1519688773008832307ll)};
  // Because we can run this in different time zones, actual time
  // (hours and minutes) could vary. Make it a generic check. In PST, outputs
  // "15:46:13.008832"
  auto s = ei.formatTime();
  EXPECT_EQ(15, s.size());
  EXPECT_TRUE(std::isdigit(s[0]));
  EXPECT_TRUE(std::isdigit(s[1]));
  EXPECT_EQ(':', s[2]);
  EXPECT_TRUE(std::isdigit(s[3]));
  EXPECT_TRUE(std::isdigit(s[4]));
  EXPECT_EQ(':', s[5]);
  EXPECT_TRUE(std::isdigit(s[6]));
  EXPECT_TRUE(std::isdigit(s[7]));
  EXPECT_EQ('.', s[8]);
  for (int i = 9; i < 15; i++) {
    EXPECT_TRUE(std::isdigit(s[i]));
  }
  // Seconds and microseconds are not affected by time zone, check
  EXPECT_EQ("13", s.substr(6, 2));
  EXPECT_EQ("008832", s.substr(9, 6));
}

TEST(ValueHistory, Empty) {
  ValueHistory vh;
  auto g = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g, 0, true));
}

// Only one last value
TEST(ValueHistory, LastSet) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    vh.endSet({}, s0);
    auto s1 = vh.beginSet({}, 1);
    vh.endSet({}, s1);

    auto g = vh.beginGet({});
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_FALSE(testValue(0));
  EXPECT_TRUE(testValue(1));
}

// Last inorder set found
TEST(ValueHistory, SetWithOverlaps1) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    auto s1 = vh.beginSet({}, 1);
    vh.endSet({}, s0);
    auto s2 = vh.beginSet({}, 2);
    vh.endSet({}, s1);
    auto g = vh.beginGet({});
    vh.beginSet({}, 3); // Set without end
    vh.endSet({}, s2);
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_TRUE(testValue(0));
  EXPECT_TRUE(testValue(1));
  EXPECT_TRUE(testValue(2));
  EXPECT_TRUE(testValue(3));
  EXPECT_FALSE(testValue(4));
}

// No last inorder set
TEST(ValueHistory, SetWithOverlaps2) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    auto g = vh.beginGet({});
    auto s1 = vh.beginSet({}, 1);
    vh.endSet({}, s0);
    vh.beginSet({}, 2); // Set without end
    vh.endSet({}, s1);
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_TRUE(testValue(0));
  EXPECT_TRUE(testValue(1));
  EXPECT_TRUE(testValue(2));
  EXPECT_FALSE(testValue(3));
}

// Test example from ValueHistory.h doc comment (see explanation there).
// Two inorder sets, overlaps with both, inorder override
TEST(ValueHistory, SetWithOverlaps3) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    auto s1 = vh.beginSet({}, 1);
    vh.endSet({}, s0);
    auto s2 = vh.beginSet({}, 2);
    vh.endSet({}, s2);
    auto s3 = vh.beginSet({}, 3);
    vh.endSet({}, s1);
    auto g = vh.beginGet({});
    vh.endSet({}, s3);
    auto s4 = vh.beginSet({}, 4);
    vh.endSet({}, s4);
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_FALSE(testValue(0)); // @s2 overrides it
  EXPECT_TRUE(testValue(1));
  EXPECT_TRUE(testValue(2));
  EXPECT_TRUE(testValue(3));
  EXPECT_TRUE(testValue(4));
  EXPECT_FALSE(testValue(5));
}

TEST(ValueHistory, Delete) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    vh.endSet({}, s0);
    auto s1 = vh.beginDelete({});
    vh.endDelete({}, s1);
    auto g = vh.beginGet({});
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_FALSE(testValue(0));
  EXPECT_FALSE(testValue(1));
}

TEST(ValueHistory, DeleteWithOverlaps) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    vh.endSet({}, s0);
    auto s1 = vh.beginDelete({});
    vh.beginSet({}, 1); // Set without end
    vh.endDelete({}, s1);
    auto g = vh.beginGet({});
    vh.beginSet({}, 2); // Set without end
    return vh.endGet({}, g, valueToGet, true);
  };

  EXPECT_FALSE(testValue(0)); // Overridden by delete
  EXPECT_TRUE(testValue(1));
  EXPECT_TRUE(testValue(2));
  EXPECT_FALSE(testValue(3));
}

TEST(ValueHistory, GetNone) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    auto s0 = vh.beginSet({}, 0);
    vh.endSet({}, s0);
    auto g0 = vh.beginGet({});
    auto g1 = vh.beginGet({});
    EXPECT_TRUE(vh.endGet({}, g0, 0, true));
    EXPECT_TRUE(vh.endGet({}, g1, 0, false));
    auto g2 = vh.beginGet({});
    return vh.endGet({}, g2, valueToGet, true);
  };

  EXPECT_FALSE(testValue(0));
  EXPECT_FALSE(testValue(1));
}

// Test it catches the problem we have seen in the real life:
// t2: set begin
// t2: set end 1
// t1: get begin
// t1: get end 1
// t6: get begin
// t6: get end null
// t3: get begin
// t3: get end 1 <-- inconsistent
TEST(ValueHistory, CatchProblem) {
  ValueHistory vh;
  auto s0 = vh.beginSet({}, 0);
  vh.endSet({}, s0);
  auto g0 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g0, 0, true));
  auto g1 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g1, 0, false));
  auto g2 = vh.beginGet({});
  EXPECT_FALSE(vh.endGet({}, g2, 0, true));
}

TEST(ValueHistory, Cleanup) {
  auto testValue = [](uint64_t valueToGet) {
    ValueHistory vh;
    for (uint32_t i = 0; i < ValueHistory::kCapacity; i++) {
      auto s0 = vh.beginSet({}, 2 * i);
      auto s1 = vh.beginSet({}, 2 * i + 1);
      vh.endSet({}, s0);
      vh.endSet({}, s1);
    }
    auto g = vh.beginGet({});
    return vh.endGet({}, g, valueToGet, true);
  };

  // Two last values are consistent:
  EXPECT_TRUE(testValue(ValueHistory::kCapacity * 2 - 1));
  EXPECT_TRUE(testValue(ValueHistory::kCapacity * 2 - 2));
  EXPECT_FALSE(testValue(ValueHistory::kCapacity * 2 - 3));
}

TEST(ValueHistory, IncompeleteSetOutsideInOrder1) {
  ValueHistory vh;
  vh.beginSet({}, 0); // Set without end
  auto g0 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g0, 0, false));
  auto g1 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g1, 0, true));
}

// Add another in order SET in addition to IncompeleteSetOutsideInOrder1,
// which should be ignored.
TEST(ValueHistory, IncompeleteSetOutsideInOrder2) {
  ValueHistory vh;
  vh.beginSet({}, 0); // Set without end
  auto s1 = vh.beginSet({}, 1);
  vh.endSet({}, s1);
  auto g0 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g0, 1, false));
  auto g1 = vh.beginGet({});
  EXPECT_TRUE(vh.endGet({}, g1, 0, true));
  // It is in order to MISS and can't be visible
  EXPECT_FALSE(vh.endGet({}, g1, 1, true));
}
} // namespace tests
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

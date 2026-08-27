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

#include "cachelib/cachebench/util/MemoryMonitorScript.h"
#include "cachelib/common/Utils.h"

namespace facebook::cachelib::cachebench {
namespace {

constexpr size_t kGB = 1024ULL * 1024ULL * 1024ULL;

TEST(MemoryMonitorScriptTest, AdvancesPhasesAndRepeatsFinalValue) {
  auto now = std::chrono::steady_clock::time_point{};
  MemoryMonitorScript script{{{1, 20}, {3, 10}, {2, 20}},
                             std::chrono::milliseconds{10},
                             false,
                             [&now] { return now; }};

  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{19};
  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{1};
  EXPECT_EQ(3 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{10};
  EXPECT_EQ(2 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{100};
  EXPECT_EQ(2 * kGB, script.getCurrentValueBytes());
}

TEST(MemoryMonitorScriptTest, RepeatsAllPhases) {
  auto now = std::chrono::steady_clock::time_point{};
  MemoryMonitorScript script{{{1, 20}, {3, 10}},
                             std::chrono::milliseconds{10},
                             true,
                             [&now] { return now; }};

  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{20};
  EXPECT_EQ(3 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{10};
  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{20};
  EXPECT_EQ(3 * kGB, script.getCurrentValueBytes());
}

TEST(MemoryMonitorScriptTest, UsesPhaseDuration) {
  auto now = std::chrono::steady_clock::time_point{};
  MemoryMonitorScript script{{{1, 11}, {2, 10}},
                             std::chrono::milliseconds{10},
                             false,
                             [&now] { return now; }};

  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{10};
  EXPECT_EQ(1 * kGB, script.getCurrentValueBytes());
  now += std::chrono::milliseconds{1};
  EXPECT_EQ(2 * kGB, script.getCurrentValueBytes());
}

TEST(MemoryMonitorScriptTest, InstallsProcessWideOverrides) {
  auto now = std::chrono::steady_clock::time_point{};

  {
    MemoryMonitorScript script{{{1, 10}},
                               std::chrono::milliseconds{10},
                               false,
                               [&now] { return now; }};
    script.install(MemoryMonitorScript::Target::MemAvailable);
    EXPECT_EQ(kGB, util::getMemAvailable());
  }
  EXPECT_GT(util::getMemAvailable(), 0);

  {
    MemoryMonitorScript script{{{2, 10}},
                               std::chrono::milliseconds{10},
                               false,
                               [&now] { return now; }};
    script.install(MemoryMonitorScript::Target::RSS);
    EXPECT_EQ(2 * kGB, util::getRSSBytes());
  }
  EXPECT_GT(util::getRSSBytes(), 0);
}

} // namespace
} // namespace facebook::cachelib::cachebench

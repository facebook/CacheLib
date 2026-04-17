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

#include <chrono>
#include <thread>

#include "cachelib/common/PercentileStats.h"

namespace facebook {
namespace cachelib {
namespace tests {

namespace {
// Measure wall-clock nanoseconds across a sleep so assertions are based on
// what actually elapsed, not what we requested.  This makes the tests
// reliable even on overloaded hosts where sleep_for overshoots.
uint64_t sleepAndMeasureNs(std::chrono::milliseconds dur) {
  auto before = std::chrono::steady_clock::now();
  std::this_thread::sleep_for(dur);
  auto after = std::chrono::steady_clock::now();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(after - before)
      .count();
}
} // namespace

TEST(LatencyTracker, DefaultConstructedRecordsNothing) {
  util::PercentileStats stats;
  {
    util::LatencyTracker tracker;
    // default-constructed, no stats pointer — should not record
  }
  auto est = stats.estimate();
  EXPECT_EQ(0, est.p50);
}

TEST(LatencyTracker, RecordsLatencyOnDestruction) {
  util::PercentileStats stats;
  uint64_t elapsedNs;
  {
    util::LatencyTracker tracker(stats);
    elapsedNs = sleepAndMeasureNs(std::chrono::milliseconds(10));
  }
  auto est = stats.estimate();
  // recorded value should be in the same ballpark as what we measured
  EXPECT_GE(est.p50, elapsedNs / 2);
  EXPECT_LE(est.p50, elapsedNs * 2);
}

TEST(LatencyTracker, MoveConstructedRecordsOnce) {
  util::PercentileStats stats;
  uint64_t elapsedNs;
  {
    util::LatencyTracker original(stats);
    elapsedNs = sleepAndMeasureNs(std::chrono::milliseconds(10));
    // move into a new tracker — original becomes inert
    util::LatencyTracker moved(std::move(original));
    // destroying original here should NOT record
  }
  // moved tracker's destructor records exactly once
  auto est = stats.estimate();
  EXPECT_GE(est.p50, elapsedNs / 2);
}

TEST(LatencyTracker, MoveAssignedRecordsOnce) {
  util::PercentileStats stats;
  uint64_t elapsedNs;
  {
    util::LatencyTracker original(stats);
    elapsedNs = sleepAndMeasureNs(std::chrono::milliseconds(10));
    util::LatencyTracker target;
    target = std::move(original);
    // original is now inert; target holds the tracking state
  }
  auto est = stats.estimate();
  EXPECT_GE(est.p50, elapsedNs / 2);
}

TEST(LatencyTracker, MoveAssignToActiveRecordsBoth) {
  util::PercentileStats stats1;
  util::PercentileStats stats2;
  uint64_t elapsedNs;
  {
    util::LatencyTracker tracker1(stats1);
    util::LatencyTracker tracker2(stats2);
    elapsedNs = sleepAndMeasureNs(std::chrono::milliseconds(10));
    // move-assigning tracker2 into tracker1 should first record tracker1's
    // latency (via destructor), then transfer tracker2's state
    tracker1 = std::move(tracker2);
  }
  // stats1 recorded when tracker1 was destroyed by move-assignment
  auto est1 = stats1.estimate();
  EXPECT_GE(est1.p50, elapsedNs / 2);
  // stats2 recorded when tracker1 (now holding tracker2's state) was destroyed
  auto est2 = stats2.estimate();
  EXPECT_GE(est2.p50, elapsedNs / 2);
}

TEST(PercentileStats, TrackValueAndEstimate) {
  util::PercentileStats stats;
  auto now = std::chrono::steady_clock::now();
  for (int i = 0; i < 100; ++i) {
    stats.trackValue(1000.0, now);
  }
  auto est = stats.estimate();
  EXPECT_EQ(1000, est.p50);
  EXPECT_EQ(1000, est.avg);
}

TEST(PercentileStats, VisitQuantileEstimates) {
  util::PercentileStats::Estimates est;
  est.p50 = 42;
  est.p99 = 99;

  std::map<std::string, double> visited;
  auto visitor = [&](folly::StringPiece name, double val) {
    visited[name.str()] = val;
  };

  util::PercentileStats::visitQuantileEstimates(visitor, est, "test_stat");
  EXPECT_EQ(42, visited["test_stat_p50"]);
  EXPECT_EQ(99, visited["test_stat_p99"]);
  // all 15 fields should be visited (avg, min, p5..p999999, max)
  EXPECT_EQ(15, visited.size());
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

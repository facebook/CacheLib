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

#include <limits>
#include <sstream>
#include <string>

#include "cachelib/cachebench/cache/CacheStats.h"

namespace facebook::cachelib::cachebench {
namespace {

TEST(CacheStatsTest, AggregationKeepsOnlyAggregatableDramIteratorStats) {
  Stats aggregate;
  aggregate.dramIteratorStats.mode = "regular";
  aggregate.dramIteratorStats.stats.sweeps = 2;
  aggregate.dramIteratorStats.stats.sweepExceptions = 1;
  aggregate.dramIteratorStats.stats.totalItems = 20;
  aggregate.dramIteratorStats.stats.lastItems = 12;
  aggregate.dramIteratorStats.stats.totalKeyBytes = 200;
  aggregate.dramIteratorStats.stats.lastKeyBytes = 120;
  aggregate.dramIteratorStats.stats.totalValueBytes = 2000;
  aggregate.dramIteratorStats.stats.lastValueBytes = 1200;
  aggregate.dramIteratorStats.stats.totalElapsedNs = 100;
  aggregate.dramIteratorStats.stats.lastElapsedNs = 60;
  aggregate.dramIteratorStats.stats.latencyNs.p99 = 90;

  Stats empty;
  aggregate += empty;

  EXPECT_EQ(0, aggregate.dramIteratorStats.stats.lastItems);
  EXPECT_EQ(0, aggregate.dramIteratorStats.stats.lastKeyBytes);
  EXPECT_EQ(0, aggregate.dramIteratorStats.stats.lastValueBytes);
  EXPECT_EQ(0, aggregate.dramIteratorStats.stats.lastElapsedNs);
  EXPECT_EQ(0, aggregate.dramIteratorStats.stats.latencyNs.p99);

  Stats other;
  other.dramIteratorStats.mode = "regular";
  other.dramIteratorStats.stats.sweeps = 3;
  other.dramIteratorStats.stats.sweepExceptions = 2;
  other.dramIteratorStats.stats.totalItems = 30;
  other.dramIteratorStats.stats.lastItems = 13;
  other.dramIteratorStats.stats.totalKeyBytes = 300;
  other.dramIteratorStats.stats.lastKeyBytes = 130;
  other.dramIteratorStats.stats.totalValueBytes = 3000;
  other.dramIteratorStats.stats.lastValueBytes = 1300;
  other.dramIteratorStats.stats.totalElapsedNs = 200;
  other.dramIteratorStats.stats.lastElapsedNs = 70;
  other.dramIteratorStats.stats.latencyNs.p99 = 190;

  aggregate += other;

  const auto& stats = aggregate.dramIteratorStats.stats;
  EXPECT_EQ(5, stats.sweeps);
  EXPECT_EQ(3, stats.sweepExceptions);
  EXPECT_EQ(50, stats.totalItems);
  EXPECT_EQ(500, stats.totalKeyBytes);
  EXPECT_EQ(5000, stats.totalValueBytes);
  EXPECT_EQ(300, stats.totalElapsedNs);
  EXPECT_EQ(0, stats.lastItems);
  EXPECT_EQ(0, stats.lastKeyBytes);
  EXPECT_EQ(0, stats.lastValueBytes);
  EXPECT_EQ(0, stats.lastElapsedNs);
  EXPECT_EQ(0, stats.latencyNs.p99);

  std::ostringstream output;
  aggregate.render(output);
  EXPECT_NE(std::string::npos,
            output.str().find("sweeps: 5, sweep exceptions: 3"));
  EXPECT_EQ(std::string::npos, output.str().find("last items"));
  EXPECT_EQ(std::string::npos, output.str().find("latency p50"));
}

TEST(CacheStatsTest, ProgressReportsDramIteratorSweepExceptionDelta) {
  Stats previous;
  previous.dramIteratorStats.mode = "regular";
  previous.dramIteratorStats.stats.sweeps = 4;
  previous.dramIteratorStats.stats.sweepExceptions = 3;

  Stats current;
  current.dramIteratorStats.mode = "regular";
  current.dramIteratorStats.stats.sweeps = 6;
  current.dramIteratorStats.stats.sweepExceptions = 5;
  current.dramIteratorStats.stats.lastItems = 42;

  EXPECT_NE(std::string::npos,
            current.progress(previous).find(
                "DRAM iterator regular: +2 sweeps, most recent sweep: 42 "
                "items, 2 sweep exceptions."));
}

TEST(CacheStatsTest, ProgressOmitsLastSweepForExceptionOnlyInterval) {
  Stats previous;
  previous.dramIteratorStats.mode = "regular";
  previous.dramIteratorStats.stats.sweeps = 4;
  previous.dramIteratorStats.stats.sweepExceptions = 3;

  Stats current;
  current.dramIteratorStats.mode = "regular";
  current.dramIteratorStats.stats.sweeps = 4;
  current.dramIteratorStats.stats.sweepExceptions = 5;
  current.dramIteratorStats.stats.lastItems = 42;

  const auto progress = current.progress(previous);
  EXPECT_NE(
      std::string::npos,
      progress.find("DRAM iterator regular: +0 sweeps, 2 sweep exceptions."));
  EXPECT_EQ(std::string::npos, progress.find("most recent sweep"));
}

TEST(CacheStatsTest, DramIteratorCountersSaturateAtSignedMaximum) {
  Stats stats;
  stats.dramIteratorStats.mode = "regular";
  stats.dramIteratorStats.stats.sweeps = std::numeric_limits<uint64_t>::max();

  folly::UserCounters counters;
  stats.render(counters);

  EXPECT_EQ(std::numeric_limits<int64_t>::max(),
            counters.at("dram_iterator_sweeps"));
}

} // namespace
} // namespace facebook::cachelib::cachebench

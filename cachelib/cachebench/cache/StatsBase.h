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

#pragma once

#include <folly/Benchmark.h>
#include <folly/logging/xlog.h>

#include <map>
#include <ostream>
#include <string>

namespace facebook {
namespace cachelib {
namespace cachebench {

/**
 * Interface for cache statistics. Different stressors can use different stats
 * implementations while providing a common interface for rendering and
 * calculating hit rates.
 */
class StatsBase {
 public:
  virtual ~StatsBase() = default;

  /**
   * Add the stats from another stats object to this one. It should only ever be
   * called with two of the same sub-types, i.e., you don't have to deal with
   * trying to aggregate child class A and child class B.
   *
   * @param otherBase other stats object
   * @return reference to this
   */
  virtual StatsBase& operator+=(const StatsBase& otherBase) = 0;

  /**
   * Get a string with some brief summary statistics for printing progress.
   * @return progress string
   */
  virtual std::string progress(const StatsBase& prevStats) const = 0;

  /**
   * Render the current stats to the output stream.
   * @param out The output stream to write to.
   */
  virtual void render(std::ostream& out) const = 0;

  /**
   * Render stats as a delta from previous stats to show changes over time.
   * @param prevStats The previous stats snapshot to calculate deltas from.
   * @param out The output stream to write to.
   */
  virtual void render(const StatsBase& prevStats, std::ostream& out) const = 0;

  /**
   * Render stats to folly::UserCounters for benchmark integration.
   * @param counters The counter map to populate.
   */
  virtual void render(folly::UserCounters& counters) const = 0;

  /**
   * Calculate hit rates as deltas from previous stats.
   * @param prevStats The previous stats snapshot to calculate deltas from.
   * @return Map of hit rate metric names to their delta values (0.0 - 100.0).
   */
  virtual std::map<std::string, double> getHitRatios(
      const StatsBase& prevStats) const = 0;

  /**
   * Render test pass/fail status and return whether the test passed.
   * @param out The output stream to write status messages to.
   * @return true if the test passed, false otherwise.
   */
  virtual bool renderIsTestPassed(std::ostream& out) const = 0;

  /**
   * Helper to cast a StatsBase to a child. Returns a nullptr if not casting to
   * the correct type.
   *
   * @return this cast to a child type pointer
   */
  template <typename ChildT>
  ChildT* asPtr() const {
    return dynamic_cast<ChildT*>(this);
  }

  /**
   * Helper to cast a StatsBase to a child. Will crash if not casting to the
   * correct type.
   *
   * @return this cast to a child type reference
   */
  template <typename ChildT>
  const ChildT& as() const {
    auto* ret = asPtr<const ChildT>();
    XCHECK_NE(ret, nullptr);
    return *ret;
  }
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

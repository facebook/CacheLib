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

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/common/Utils.h"

/**
 * Counters for CacheComponents.
 *
 * Counter types are templated to allow differentiating between collecting
 * counters (i.e., optimized for high update throughput) and reporting counters
 * (i.e., easy to print).
 *
 * Users should instantiate CacheComponentStatsCollector in the component for
 * stats collection during normal usage.  Users should return
 * CacheComponentStats for reporting & printing.  CacheComponentStats structs
 * can be constructed from a const CacheComponentStatsCollector&.
 */

namespace facebook::cachelib::interface {
namespace detail {
/**
 * Cheap thread-local sampling function. Allows downsampling latency counters on
 * super hot events (e.g., find()) to avoid overhead.  sampleRate = 0 means
 * never sample.
 */
bool shouldSample(size_t sampleRate);

/**
 * Counters for tracking the throughput, goodput and error rates of a cache
 * operation.
 */
template <typename T>
struct OpThroughputCounters {
  T calls_;
  T successes_;
  T errors_;

  OpThroughputCounters() : calls_(0), successes_(0), errors_(0) {}

  template <typename U>
  explicit OpThroughputCounters(const OpThroughputCounters<U>& other)
      : calls_(other.calls_.get()),
        successes_(other.successes_.get()),
        errors_(other.errors_.get()) {}
};

/**
 * Adds hit & miss counters to OpThroughputCounters for APIs that do lookups.
 */
template <typename T>
struct OpFindThroughputCounters : public OpThroughputCounters<T> {
  T hits_;
  T misses_;

  OpFindThroughputCounters() : hits_(0), misses_(0) {}

  template <typename U>
  explicit OpFindThroughputCounters(const OpFindThroughputCounters<U>& other)
      : OpThroughputCounters<T>(other),
        hits_(other.hits_.get()),
        misses_(other.misses_.get()) {}
};

/**
 * Counters for measuring latency of cache operations.
 */
struct LatencyMeasurementCounter {
  // RAII guard for measuring latency
  struct LatencyGuard {
   public:
    explicit LatencyGuard(util::PercentileStats& stats)
        : start_(std::chrono::steady_clock::now()), stats_(stats) {}
    ~LatencyGuard() {
      stats_.trackValue((std::chrono::steady_clock::now() - start_).count());
    }

    LatencyGuard(const LatencyGuard&) = delete;
    LatencyGuard& operator=(const LatencyGuard&) = delete;
    LatencyGuard(LatencyGuard&&) = delete;
    LatencyGuard& operator=(LatencyGuard&&) = delete;

   private:
    std::chrono::steady_clock::time_point start_;
    util::PercentileStats& stats_;
  };

  /**
   * Only bump latency counters every 1/sampleRate calls. Setting to 0 means
   * never sample this latency counter.
   *
   * @param sampleRate 1/sampleRate calls will bump latency counters
   */
  LatencyMeasurementCounter(size_t sampleRate = 1) : sampleRate_(sampleRate) {}

  /**
   * Helper to automatically measure latency & update counters in a scope.
   * Returns an non-empty std::optional if sampled for latency measurement.
   *
   * @return std::nullopt if not sampled, LatencyGuard if sampled
   */
  [[nodiscard]] std::optional<LatencyGuard> start() {
    if (shouldSample(sampleRate_)) {
      return std::make_optional<LatencyGuard>(latency_);
    } else {
      return std::nullopt;
    }
  }

  /**
   * Estimate percentiles for reporting
   * @return PercentileStats::Estimates with nanosecond latency percentiles
   */
  util::PercentileStats::Estimates toEstimates() const {
    return latency_.estimate();
  }

  mutable util::PercentileStats latency_;

 private:
  const size_t sampleRate_;
};

/**
 * Statistics for a cache component. Includes throughput and latency counters
 * for each operation that can be performed by a cache component.
 */
template <typename ThroughputType, typename OpLatencyType>
struct CacheComponentStatsImpl {
  /**
   * Configure a per-operation latency counter sampling rate. Samples 1/N
   * operations for bumping latency counters. Setting to 0 means never sample.
   */
  struct LatencySamplingConfig {
    size_t allocate_{1};
    size_t insert_{1};
    size_t insertOrReplace_{1};
    size_t find_{1};
    size_t findToWrite_{1};
    size_t removeByKey_{1};
    size_t removeByHandle_{1};
    size_t writeBack_{1};
    size_t release_{1};
  };

  template <typename OpThroughputType>
  struct OpCounters {
    OpThroughputType throughput_;
    OpLatencyType latency_;

    explicit OpCounters(size_t latencySampleRate)
        : latency_{latencySampleRate} {}

    template <typename U>
    explicit OpCounters(const U& other)
        : throughput_(other.throughput_),
          latency_(other.latency_.toEstimates()) {}
  };

  OpCounters<OpThroughputCounters<ThroughputType>> allocate_;
  OpCounters<OpThroughputCounters<ThroughputType>> insert_;
  OpCounters<OpThroughputCounters<ThroughputType>> insertOrReplace_;
  OpCounters<OpFindThroughputCounters<ThroughputType>> find_;
  OpCounters<OpFindThroughputCounters<ThroughputType>> findToWrite_;
  OpCounters<OpFindThroughputCounters<ThroughputType>> removeByKey_;
  OpCounters<OpThroughputCounters<ThroughputType>> removeByHandle_;
  OpCounters<OpThroughputCounters<ThroughputType>> writeBack_;
  OpCounters<OpThroughputCounters<ThroughputType>> release_;

  explicit CacheComponentStatsImpl(
      const LatencySamplingConfig& samplingConfig = {})
      : allocate_(samplingConfig.allocate_),
        insert_(samplingConfig.insert_),
        insertOrReplace_(samplingConfig.insertOrReplace_),
        find_(samplingConfig.find_),
        findToWrite_(samplingConfig.findToWrite_),
        removeByKey_(samplingConfig.removeByKey_),
        removeByHandle_(samplingConfig.removeByHandle_),
        writeBack_(samplingConfig.writeBack_),
        release_(samplingConfig.release_) {}

  template <typename U>
  explicit CacheComponentStatsImpl(const U& other)
      : allocate_(other.allocate_),
        insert_(other.insert_),
        insertOrReplace_(other.insertOrReplace_),
        find_(other.find_),
        findToWrite_(other.findToWrite_),
        removeByKey_(other.removeByKey_),
        removeByHandle_(other.removeByHandle_),
        writeBack_(other.writeBack_),
        release_(other.release_) {}
};

using ReportStats =
    CacheComponentStatsImpl<size_t, util::PercentileStats::Estimates>;

} // namespace detail

// Data type for collecting stats in high throughput scenarios
using CacheComponentStatsCollector =
    detail::CacheComponentStatsImpl<TLCounter,
                                    detail::LatencyMeasurementCounter>;

// Data type for reporting
struct CacheComponentStats : public detail::ReportStats {
  CacheComponentStats() = default;
  template <typename U>
  explicit CacheComponentStats(const U& other) : detail::ReportStats(other) {}

  size_t numItems{0};
  util::StatsMap extraStats_;
};

} // namespace facebook::cachelib::interface

namespace std {
std::ostream& operator<<(
    std::ostream& os,
    const facebook::cachelib::interface::CacheComponentStats& stats);
}

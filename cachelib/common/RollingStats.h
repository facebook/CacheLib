/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <folly/Range.h>
#include <folly/logging/xlog.h>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace util {

class RollingStats {
 public:
  // track latency by taking the value of duration directly.
  void trackValue(double value) {
    // This is a highly unlikely scenario where
    // cnt_ reaches numerical limits. Skip update
    // of the rolling average anymore.
    if (cnt_ == std::numeric_limits<uint64_t>::max()) {
      cnt_ = 0;
      return;
    }
    auto ratio = static_cast<double>(cnt_) / (cnt_ + 1);
    avg_ *= ratio;
    ++cnt_;
    avg_ += value / cnt_;
  }

  // Return the rolling average.
  double estimate() { return avg_; }

 private:
  double avg_{0};
  uint64_t cnt_{0};
};

class RollingLatencyTracker {
 public:
  explicit RollingLatencyTracker(RollingStats& stats)
      : stats_(&stats), begin_(std::chrono::steady_clock::now()) {}
  RollingLatencyTracker() {}
  ~RollingLatencyTracker() {
    if (stats_) {
      auto tp = std::chrono::steady_clock::now();
      auto diffNanos =
          std::chrono::duration_cast<std::chrono::nanoseconds>(tp - begin_)
              .count();
      stats_->trackValue(static_cast<double>(diffNanos));
    }
  }

  RollingLatencyTracker(const RollingLatencyTracker&) = delete;
  RollingLatencyTracker& operator=(const RollingLatencyTracker&) = delete;

  RollingLatencyTracker(RollingLatencyTracker&& rhs) noexcept
      : stats_(rhs.stats_), begin_(rhs.begin_) {
    rhs.stats_ = nullptr;
  }

  RollingLatencyTracker& operator=(RollingLatencyTracker&& rhs) noexcept {
    if (this != &rhs) {
      this->~RollingLatencyTracker();
      new (this) RollingLatencyTracker(std::move(rhs));
    }
    return *this;
  }

 private:
  RollingStats* stats_{nullptr};
  std::chrono::time_point<std::chrono::steady_clock> begin_;
};
} // namespace util
} // namespace cachelib
} // namespace facebook

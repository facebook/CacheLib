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

#include "cachelib/common/EventTracker.h"

namespace facebook {
namespace cachelib {

void EventTracker::setSamplingRate(uint32_t samplingRate) {
  samplingRate_ = samplingRate;
}

EventTracker::EventTracker(Config&& config)
    : samplingRate_(config.samplingRate), config_(std::move(config)) {
  validateConfig(config_);
}

void EventTracker::validateConfig(const Config& config) {
  if (config.samplingRate < 1) {
    throw std::invalid_argument(
        "EventTracker config validation failed: samplingRate must be >= 1");
  }
}

uint32_t EventTracker::sampleKey(folly::StringPiece key,
                                 uint32_t samplingRate) {
  if (samplingRate == 0) {
    return 0;
  }
  return furcHash(key.data(), key.size(), samplingRate) == 0 ? samplingRate : 0;
}

void EventTracker::record(const EventInfo& eventInfo) {
  if (sampleKey(eventInfo.key, samplingRate_) == 0) {
    return;
  }
  config_.eventSink->recordEvent(eventInfo);
}

} // namespace cachelib
} // namespace facebook

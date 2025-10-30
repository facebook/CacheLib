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

#include <common/hash/FurcHash.h>

#include "cachelib/common/EventSink.h"

namespace facebook::cachelib {

class EventTracker {
 public:
  struct Config {
    uint32_t samplingRate = 0;
    std::unique_ptr<EventSink> eventSink = nullptr;
  };

  explicit EventTracker(Config&& config);

  void setSamplingRate(uint32_t samplingRate);

  void record(const EventInfo& eventInfo);

 private:
  static void validateConfig(const Config& config);
  static uint32_t sampleKey(folly::StringPiece key, uint32_t samplingRate);

  std::atomic<uint32_t> samplingRate_;
  Config config_;
};

} // namespace facebook::cachelib

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

#include "cachelib/common/EventTracker.h"

using namespace ::testing;
using namespace facebook::cachelib;

TEST(EventTrackerTest, BasicLogging) {
  const char* key = "sample_k0";
  const uint32_t valueSize = 100;
  const uint32_t ttlSecs = 10;
  const uint32_t numItems = 2;
  uint32_t samplingRate = 1;
  std::vector<EventInfo> events;
  for (uint64_t i = 0; i < numItems; i++) {
    EventInfo eventInfo;
    eventInfo.key = key;
    eventInfo.event = AllocatorApiEvent::FIND;
    eventInfo.result = (i % 2 == 0) ? AllocatorApiResult::NOT_FOUND
                                    : AllocatorApiResult::FOUND;
    eventInfo.size = valueSize;
    eventInfo.ttl = ttlSecs;
    events.push_back(eventInfo);
  }

  EventTracker::Config config;
  config.samplingRate = samplingRate;
  auto eventTracker = std::make_unique<EventTracker>(std::move(config));
  for (uint64_t i = 0; i < numItems; i++) {
    eventTracker->record(events.at(i));
  }
}

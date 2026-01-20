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

#include <folly/MPMCQueue.h>
#include <folly/container/F14Map.h>

#include <thread>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/EventSink.h"
#include "cachelib/common/Hash.h"

namespace facebook::cachelib {

enum class RecordResult { NOT_SAMPLED, QUEUED, QUEUE_FULL };

inline const char* toString(RecordResult result) {
  switch (result) {
  case RecordResult::NOT_SAMPLED:
    return "NOT_SAMPLED";
  case RecordResult::QUEUED:
    return "QUEUED";
  case RecordResult::QUEUE_FULL:
    return "QUEUE_FULL";
  default:
    return "UNKNOWN";
  }
}

class EventTracker {
 public:
  struct Config {
    uint32_t samplingRate = 0;
    uint32_t queueSize = 0;
    std::unique_ptr<EventSink> eventSink = nullptr;
    std::function<void(EventInfo&)> eventInfoCallback = nullptr;
  };

  explicit EventTracker(Config&& config);
  ~EventTracker();

  void setSamplingRate(uint32_t samplingRate);

  uint32_t getSamplingRate() const;

  // This calls sampleKey and if sampled, adds the event
  // to the queue.
  RecordResult record(const EventInfo& eventInfo);

  // Sample key and track stats such as sample attempts
  // and sucess. This function is also called from
  // record. If you want one sample call per event
  // then consider using sampleKey + recordWithoutSampling.
  bool sampleKey(folly::StringPiece key);

  // For users who want to first call sampleKey and then call
  // recordWithoutSampling() if sampleKey returns true to avoid
  // allocating EventInfo object when it is not going to be sampled.
  RecordResult recordWithoutSampling(const EventInfo& eventInfo);

  void getStats(folly::F14FastMap<std::string, uint64_t>& statsMap) const;

 private:
  void validateConfig();
  void runBackgroundThread();

  std::thread backgroundThread_;
  folly::MPMCQueue<EventInfo> eventInfoQueue_;

  std::atomic<uint32_t> samplingRate_;
  std::unique_ptr<EventSink> eventSink_;
  std::function<void(EventInfo&)> eventInfoCallback_;

  AtomicCounter recordCount_{0};
  AtomicCounter sampleAttemptCount_{0};
  AtomicCounter sampleSuccessCount_{0};
  AtomicCounter dropCount_{0};
  AtomicCounter addToQueueCount_{0};
};

} // namespace facebook::cachelib

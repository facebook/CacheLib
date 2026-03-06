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

EventTracker::EventTracker(Config&& config)
    : eventInfoQueue_(config.queueSize),
      eventSink_(std::move(config.eventSink)),
      preQueueCallback_(std::move(config.preQueueCallback)),
      postQueueCallback_(std::move(config.postQueueCallback)),
      sampler_(config.sampler ? std::move(config.sampler)
                              : std::make_unique<FurcHashSampler>(0)) {
  validateConfig();
  backgroundThread_ = std::thread([this]() { runBackgroundThread(); });
}

EventTracker::~EventTracker() {
  eventInfoQueue_.blockingWrite(EventInfo());
  backgroundThread_.join();
}

void EventTracker::validateConfig() {
  // If config.queueSize < 1, then eventInfoQueue_ should throw.
  if (eventSink_ == nullptr) {
    throw std::invalid_argument(
        "EventTracker config validation failed: eventSink must not be null");
  }
}

bool EventTracker::sampleKey(folly::StringPiece key) {
  sampleAttemptCount_.inc();

  if (sampler_->shouldSample(key)) {
    sampleSuccessCount_.inc();
    return true;
  }
  return false;
}

void EventTracker::runBackgroundThread() {
  try {
    while (true) {
      EventInfo eventInfo;
      eventInfoQueue_.blockingRead(eventInfo);
      if (!eventInfo.key.empty()) {
        if (postQueueCallback_) {
          postQueueCallback_(eventInfo);
        }
        eventSink_->recordEvent(eventInfo);
      } else {
        // received sentinel event
        break;
      }
    }
  } catch (const std::exception& e) {
    XLOG(ERR)
        << "Exception in EventTracker thread. Stopping EventTracker. Error: "
        << e.what();
  } catch (...) {
    XLOG(ERR)
        << "Unknown exception in EventTracker thread. Stopping EventTracker.";
  }
}

RecordResult EventTracker::record(EventInfo& eventInfo) {
  recordCount_.inc();
  if (!sampleKey(eventInfo.key)) {
    return RecordResult::NOT_SAMPLED;
  }
  return recordWithoutSampling(eventInfo);
}

RecordResult EventTracker::recordWithoutSampling(EventInfo& eventInfo) {
  addToQueueCount_.inc();
  if (preQueueCallback_) {
    preQueueCallback_(eventInfo);
  }
  bool addedToQueue = eventInfoQueue_.write(eventInfo);
  if (!addedToQueue) {
    dropCount_.inc();
  }
  return addedToQueue ? RecordResult::QUEUED : RecordResult::QUEUE_FULL;
}

void EventTracker::getStats(
    folly::F14FastMap<std::string, uint64_t>& statsMap) const {
  statsMap["record"] = recordCount_.get();
  statsMap["sample_attempts"] = sampleAttemptCount_.get();
  statsMap["sample_success"] = sampleSuccessCount_.get();
  statsMap["dropped"] = dropCount_.get();
  statsMap["add_to_queue"] = addToQueueCount_.get();
}

} // namespace cachelib
} // namespace facebook

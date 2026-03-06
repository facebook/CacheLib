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

// Abstract interface for custom sampling implementations.
// Implement this interface to provide custom sampling logic
// that can be injected into EventTracker without adding dependencies
// to the core CacheLib code.
class SamplerInterface {
 public:
  virtual ~SamplerInterface() = default;

  // Returns a non-zero weight (could be sampling rate) if the given key should
  // be sampled, or 0 if it should not be sampled.
  // @param key The key to check for sampling
  // @return non-zero weight if sampled, 0 otherwise
  virtual uint64_t shouldSample(folly::StringPiece key) const = 0;
};

// Default sampler implementation using furcHash-based consistent sampling.
// Samples keys deterministically based on a configurable sampling rate.
class FurcHashSampler final : public SamplerInterface {
 public:
  explicit FurcHashSampler(uint32_t samplingRate)
      : samplingRate_{samplingRate} {}

  uint64_t shouldSample(folly::StringPiece key) const override {
    uint32_t rate = getSamplingRate();
    if (rate > 0 && furcHash(key.data(), key.size(), rate) == 0) {
      return rate;
    }
    return 0;
  }

  void setSamplingRate(uint32_t samplingRate) {
    samplingRate_.store(samplingRate, std::memory_order_relaxed);
  }

  uint32_t getSamplingRate() const {
    return samplingRate_.load(std::memory_order_relaxed);
  }

 private:
  std::atomic<uint32_t> samplingRate_{0};
};

class EventTracker {
 public:
  struct Config {
    uint32_t queueSize = 0;
    std::unique_ptr<EventSink> eventSink = nullptr;

    // Callback invoked before queuing the event (in the caller's thread).
    // Use this for processing non-owning data (e.g., payload StringPiece)
    // that may not be valid by the time the background thread processes it.
    std::function<void(EventInfo&)> preQueueCallback = nullptr;

    // Callback invoked after dequeuing the event (in the background thread).
    // Use this for processing owned data (e.g., parsing the key string)
    // to avoid adding latency to the hot path.
    std::function<void(EventInfo&)> postQueueCallback = nullptr;

    // Optional custom sampler.
    // If provided, this sampler will be used to determine whether a key
    // should be sampled. If not provided, a default FurcHashSampler is
    // created with sampling rate 0 (disabled).
    std::unique_ptr<SamplerInterface> sampler = nullptr;
  };

  explicit EventTracker(Config&& config);
  ~EventTracker();

  // This calls sampleKey and if sampled, adds the event
  // to the queue.
  RecordResult record(EventInfo& eventInfo);

  // Sample key and track stats such as sample attempts
  // and sucess. This function is also called from
  // record. If you want one sample call per event
  // then consider using sampleKey + recordWithoutSampling.
  bool sampleKey(folly::StringPiece key);

  // For users who want to first call sampleKey and then call
  // recordWithoutSampling() if sampleKey returns true to avoid
  // allocating EventInfo object when it is not going to be sampled.
  RecordResult recordWithoutSampling(EventInfo& eventInfo);

  void getStats(folly::F14FastMap<std::string, uint64_t>& statsMap) const;

 private:
  void validateConfig();
  void runBackgroundThread();

  std::thread backgroundThread_;
  folly::MPMCQueue<EventInfo> eventInfoQueue_;

  std::unique_ptr<EventSink> eventSink_;
  std::function<void(EventInfo&)> preQueueCallback_;
  std::function<void(EventInfo&)> postQueueCallback_;
  std::unique_ptr<SamplerInterface> sampler_;

  AtomicCounter recordCount_{0};
  AtomicCounter sampleAttemptCount_{0};
  AtomicCounter sampleSuccessCount_{0};
  AtomicCounter dropCount_{0};
  AtomicCounter addToQueueCount_{0};
};

} // namespace facebook::cachelib

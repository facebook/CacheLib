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
#include <folly/container/F14Map.h>

#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/EventInterface.h"

namespace facebook::cachelib {

struct EventInfo {
  // Use std::string for owned fields (app, usecase), and folly::StringPiece for
  // cacheName, which is managed by the allocator and guaranteed to outlive
  // EventInfo.
  folly::StringPiece cacheName; // Non-owning reference to cache name.
  time_t eventTimestamp{0};     // Timestamp of this event.
  AllocatorApiEvent event{AllocatorApiEvent::INVALID};   // Type of the event.
  AllocatorApiResult result{AllocatorApiResult::FAILED}; // Result of the event.
  std::string key;                                       // The item's key.
  folly::Optional<size_t> size;                          // Size of the value.
  folly::Optional<uint32_t> ttl;                         // TTL of the item.
  folly::Optional<uint32_t> remainingTTL; // Remaining TTL of the item.
  folly::Optional<uint32_t> allocSize;    // Allocation class size
  folly::Optional<PoolId> poolId;         // The item's pool ID.
  folly::Optional<std::string> app;       // The item's application.
  folly::Optional<std::string> usecase;   // The item's usecase.
  folly::Optional<uint32_t> usecaseId;    // The item's usecase id.
  folly::Optional<uint32_t> appId;        // The item's app id.
  folly::Optional<
      folly::F14FastMap<std::string, std::variant<std::string, int64_t>>>
      metaInfo; // Map with additional metadata.
};

class EventTracker {
 public:
  struct Config {
    uint32_t samplingRate = 0;
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

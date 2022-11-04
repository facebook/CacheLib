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

#include <chrono>
#include <memory>

#include "cachelib/navy/block_cache/Region.h"
#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Abstract base class of an eviction policy.
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  // Adds a new region for tracking.
  // @param region    region to be tracked
  virtual void track(const Region& region) = 0;

  // Touches (record a hit) this region.
  virtual void touch(RegionId id) = 0;

  // Evicts a region and stops tracking.
  virtual RegionId evict() = 0;

  // Resets policy to the initial state.
  virtual void reset() = 0;

  // Gets memory used by the policy.
  virtual size_t memorySize() const = 0;

  // Exports policy stats via CounterVisitor.
  virtual void getCounters(const CounterVisitor&) const = 0;

  // Persists metadata associated with this policy.
  virtual void persist(RecordWriter& rw) const = 0;

  // Recovers from previously persisted metadata associated with this policy.
  virtual void recover(RecordReader& rr) = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

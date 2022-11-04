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

#include "cachelib/common/PercentileStats.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {
// Abstract base class of a reinsertion policy on block cache.
// When a region reaches eviction, every item in the region will
// be evaluated by the policy to determine whether or not to reinsert
// the item back to block cache.
class BlockCacheReinsertionPolicy {
 public:
  virtual ~BlockCacheReinsertionPolicy() = default;

  // When the region an item belongs to is evicted, figure out
  // whether the item should be inserted to block cache.
  virtual bool shouldReinsert(folly::StringPiece key) = 0;

  // Exports policy stats via CounterVisitor.
  virtual void getCounters(const util::CounterVisitor& visitor) const = 0;
};

} // namespace cachelib
} // namespace facebook

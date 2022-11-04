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

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

struct PoolOptimizeContext {
  PoolId victimPoolId{Slab::kInvalidPoolId};
  PoolId receiverPoolId{Slab::kInvalidPoolId};

  PoolOptimizeContext() = default;
  PoolOptimizeContext(PoolId victim, PoolId receiver)
      : victimPoolId(victim), receiverPoolId(receiver) {}
};

// Base class for pool optimizing strategy, providing functions to find victim
// and receiver pools.
// The goal of PoolOptimizeStrategy is to figure out which pool should release
// a slab and which pool should gain a slab, based on a user-defined set of
// criteria.
// The actual release operation is handled by PoolOptimizer
class PoolOptimizeStrategy {
 public:
  struct BaseConfig {};
  virtual void updateConfig(const BaseConfig&) {}

  enum Type { PickNothingOrTest, MarginalHits, NumTypes };
  explicit PoolOptimizeStrategy(Type strategyType = PickNothingOrTest)
      : type_(strategyType) {}
  virtual ~PoolOptimizeStrategy() = default;

  // Pick an victim and receiver pools from regular pools
  //
  // @param allocator   Cache allocator that implements CacheBase
  //
  // @return PoolOptimizeContext   contains victim and receiver
  PoolOptimizeContext pickVictimAndReceiverRegularPools(
      const CacheBase& cache) {
    return pickVictimAndReceiverRegularPoolsImpl(cache);
  }

  // Pick an victim and receiver pools from compact caches
  //
  // @param allocator   Cache allocator that implements CacheBase
  //
  // @return PoolOptimizeContext   contains victim and receiver
  PoolOptimizeContext pickVictimAndReceiverCompactCaches(
      const CacheBase& cache) {
    return pickVictimAndReceiverCompactCachesImpl(cache);
  }

  Type getType() const { return type_; }

 protected:
  virtual PoolOptimizeContext pickVictimAndReceiverRegularPoolsImpl(
      const CacheBase& /* cache */) {
    return {};
  }

  virtual PoolOptimizeContext pickVictimAndReceiverCompactCachesImpl(
      const CacheBase& /* cache */) {
    return {};
  }

  static const PoolOptimizeContext kNoOpContext;

 private:
  Type type_{NumTypes};
};

} // namespace cachelib
} // namespace facebook

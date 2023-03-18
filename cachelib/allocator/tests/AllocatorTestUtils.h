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

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {
namespace tests {
struct AlwaysPickOneRebalanceStrategy : public RebalanceStrategy {
  // Figure out which allocation class has the highest number of allocations
  // and release
 public:
  const ClassId victim;
  const ClassId receiver;
  AlwaysPickOneRebalanceStrategy(ClassId victim_, ClassId receiver_)
      : RebalanceStrategy(PickNothingOrTest),
        victim(victim_),
        receiver(receiver_) {}

 private:
  ClassId pickVictim(const CacheBase&, PoolId) { return victim; }

  ClassId pickVictimImpl(const CacheBase& allocator,
                         PoolId pid,
                         const PoolStats&) override {
    return pickVictim(allocator, pid);
  }

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& allocator,
                                             PoolId pid,
                                             const PoolStats&) override {
    return {pickVictim(allocator, pid), receiver};
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

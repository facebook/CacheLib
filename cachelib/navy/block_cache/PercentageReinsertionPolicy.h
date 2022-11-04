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

#include <folly/Random.h>

#include <cstdint>

#include "cachelib/allocator/nvmcache/BlockCacheReinsertionPolicy.h"
#include "folly/Range.h"
#include "folly/logging/xlog.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Percentage based reinsertion policy.
// This is used for testing where a certain fraction of evicted items(governed
// by the percentage) are always reinserted. The percentage value is between 0
// and 100 for reinsertion.
class PercentageReinsertionPolicy : public BlockCacheReinsertionPolicy {
 public:
  // @param percentage     reinsertion chances: 0 - 100
  explicit PercentageReinsertionPolicy(uint32_t percentage)
      : percentage_{percentage} {
    XDCHECK(percentage > 0 && percentage <= 100);
  }

  // Applies percentage based policy to determine whether or not we should keep
  // this key around longer in cache.
  bool shouldReinsert(folly::StringPiece /* key */) override {
    return folly::Random::rand32() % 100 < percentage_;
  }

  void getCounters(const util::CounterVisitor& /* visitor */) const override {}

 private:
  const uint32_t percentage_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"

#include <folly/logging/xlog.h>

#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
HitsReinsertionPolicy::HitsReinsertionPolicy(uint8_t hitsThreshold,
                                             const Index& index)
    : hitsThreshold_{hitsThreshold}, index_(index) {}

bool HitsReinsertionPolicy::shouldReinsert(folly::StringPiece key) {
  const auto lr = index_.peek(
      makeHK(
          BufferView{key.size(), reinterpret_cast<const uint8_t*>(key.data())})
          .keyHash());
  if (!lr.found() || lr.currentHits() < hitsThreshold_) {
    return false;
  }

  hitsOnReinsertionEstimator_.trackValue(lr.currentHits());
  return true;
}

void HitsReinsertionPolicy::getCounters(
    const util::CounterVisitor& visitor) const {
  hitsOnReinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_reinsertion_hits");
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

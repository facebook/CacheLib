/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

namespace facebook {
namespace cachelib {
namespace navy {
HitsReinsertionPolicy::HitsReinsertionPolicy(uint8_t hitsThreshold)
    : hitsThreshold_{hitsThreshold} {}

bool HitsReinsertionPolicy::shouldReinsert(HashedKey hk) {
  XDCHECK(index_);
  const auto lr = index_->peek(hk.keyHash());
  if (!lr.found() || lr.currentHits() < hitsThreshold_) {
    return false;
  }

  hitsOnReinsertionEstimator_.trackValue(lr.currentHits());
  return true;
}

// TODO: T95755384 delete persist API after BigCache has rolled out the release
// that no longer tries to "recover" the persistence policy.
void HitsReinsertionPolicy::persist(RecordWriter& rw) {
  // disable future recover by writing kNumLocks empty AccessTrackers
  serializeProto(kNumLocks, rw);
  for (size_t i = 0; i < kNumLocks; i++) {
    serialization::AccessTracker trackerData;
    serializeProto(trackerData, rw);
  }
}

void HitsReinsertionPolicy::getCounters(const CounterVisitor& visitor) const {
  hitsOnReinsertionEstimator_.visitQuantileEstimator(
      visitor, "navy_bc_item_reinsertion_hits");
}

HitsReinsertionPolicy::AccessStats HitsReinsertionPolicy::getAccessStats(
    HashedKey hk) const {
  XDCHECK(index_);
  const auto lr = index_->peek(hk.keyHash());
  if (lr.found()) {
    return AccessStats{lr.totalHits(), lr.currentHits()};
  }
  return {};
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

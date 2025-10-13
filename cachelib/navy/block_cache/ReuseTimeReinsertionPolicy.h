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

#include <cachelib/navy/block_cache/Index.h>
#include <folly/container/F14Map.h>

#include <tuple>

#include "cachelib/allocator/nvmcache/BlockCacheReinsertionPolicy.h"
#include "cachelib/common/AccessTracker.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"

namespace facebook::cachelib::navy {

namespace tests {
class ReuseTimeReinsertionPolicyTest_PrevAccessBucketTracking_Test;
class ReuseTimeReinsertionPolicyTest_ReuseTimeComputation_Test;
} // namespace tests

class ReuseTimeReinsertionPolicy : public BlockCacheReinsertionPolicy {
 public:
  ReuseTimeReinsertionPolicy(const cachelib::navy::Index& index,
                             size_t numBuckets,
                             size_t bucketSize,
                             uint32_t defaultReinsertionThreshold);

  ReuseTimeReinsertionPolicy(const cachelib::navy::Index& index,
                             size_t numBuckets,
                             size_t bucketSize,
                             uint32_t defaultReinsertionThreshold,
                             std::shared_ptr<Ticker> ticker);

  bool shouldReinsert(folly::StringPiece key,
                      folly::StringPiece value) override;

  void onLookup(folly::StringPiece key) override;

  void getCounters(const util::CounterVisitor& visitor) const override;

 private:
  static uint32_t isExpired(folly::StringPiece value);
  std::tuple<int64_t, int64_t> getPrevAccessBuckets(folly::StringPiece key);
  size_t getReuseTime(folly::StringPiece key);

  const cachelib::navy::Index& index_;
  size_t numBuckets_{0};
  size_t bucketSize_{0};
  uint32_t reuseTimeThreshold_{0};
  std::unique_ptr<AccessTracker> tracker_;
  AtomicCounter reinsertAttempts_{0};
  AtomicCounter keyNotFound_{0};
  AtomicCounter expired_{0};
  AtomicCounter reinserted_{0};
  AtomicCounter noPrevAccess_{0};
  mutable util::PercentileStats reuseTimeStats_;

  friend class tests::ReuseTimeReinsertionPolicyTest_ReuseTimeComputation_Test;
  friend class tests::
      ReuseTimeReinsertionPolicyTest_PrevAccessBucketTracking_Test;
};

} // namespace facebook::cachelib::navy

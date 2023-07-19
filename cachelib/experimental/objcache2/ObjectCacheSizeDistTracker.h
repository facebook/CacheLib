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

#include <folly/stats/QuantileHistogram.h>

#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename ObjectCache>
class ObjectCacheSizeDistTracker : public PeriodicWorker {
 public:
  explicit ObjectCacheSizeDistTracker(ObjectCache& objCache)
      : objCache_(objCache),
        objectSizeBytesHist_{std::make_shared<folly::QuantileHistogram<>>()} {}

  void getCounters(const util::CounterVisitor& visitor) const {
    std::shared_ptr<folly::QuantileHistogram<>> curHist;
    {
      // lock the mutex before accessing objectSizeBytesHist_
      std::lock_guard<std::mutex> l(mutex_);
      curHist = objectSizeBytesHist_;
    }

    for (auto quantile : curHist->quantiles()) {
      visitor(fmt::format("objcache.size_distribution.object_size_bytes_p{}",
                          static_cast<uint32_t>(quantile * 100)),
              curHist->estimateQuantile(quantile));
    }
    visitor("objcache.size_distribution.traverse_time_ms",
            traverseTimeMs_.load());
  }

 private:
  void work() override final {
    auto beginTime = util::getCurrentTimeMs();
    auto newHist = std::make_shared<folly::QuantileHistogram<>>();
    // scan the cache to get the object size
    for (auto itr = objCache_.begin(); itr != objCache_.end(); ++itr) {
      newHist->addValue(objCache_.getObjectSize(itr));
    }
    {
      // lock the mutex before updating objectSizeBytesHist_
      std::lock_guard<std::mutex> l(mutex_);
      objectSizeBytesHist_ = newHist;
    }

    traverseTimeMs_.store(util::getCurrentTimeMs() - beginTime,
                          std::memory_order_relaxed);
  }

  ObjectCache& objCache_;
  mutable std::mutex mutex_;
  std::shared_ptr<folly::QuantileHistogram<>> objectSizeBytesHist_{};
  //  time taken to scan the cache and build a new histogram
  std::atomic<uint64_t> traverseTimeMs_{0};
};

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

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

#include "cachelib/allocator/FreeThresholdStrategy.h"

#include <folly/logging/xlog.h>

namespace facebook::cachelib {

FreeThresholdStrategy::FreeThresholdStrategy(double lowEvictionAcWatermark,
                                             double highEvictionAcWatermark,
                                             uint64_t maxEvictionBatch,
                                             uint64_t minEvictionBatch)
    : lowEvictionAcWatermark(lowEvictionAcWatermark),
      highEvictionAcWatermark(highEvictionAcWatermark),
      maxEvictionBatch(maxEvictionBatch),
      minEvictionBatch(minEvictionBatch) {}

std::vector<size_t> FreeThresholdStrategy::calculateBatchSizes(
    const CacheBase& /* cache */,
    std::vector<MemoryDescriptorType> /* acVec */) {
  throw std::runtime_error("Not supported yet!");
}

} // namespace facebook::cachelib

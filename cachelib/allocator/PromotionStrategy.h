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

#include "cachelib/allocator/BackgroundMoverStrategy.h"
#include "cachelib/allocator/Cache.h"

namespace facebook {
namespace cachelib {

// Strategy for background promotion worker.
class PromotionStrategy : public BackgroundMoverStrategy {
 public:
  PromotionStrategy(uint64_t promotionAcWatermark,
                    uint64_t maxPromotionBatch,
                    uint64_t minPromotionBatch)
      : promotionAcWatermark(promotionAcWatermark),
        maxPromotionBatch(maxPromotionBatch),
        minPromotionBatch(minPromotionBatch) {}
  ~PromotionStrategy() {}

  std::vector<size_t> calculateBatchSizes(
      const CacheBase& cache, std::vector<MemoryDescriptorType> acVec) {
    return {};
  }

 private:
  double promotionAcWatermark{4.0};
  uint64_t maxPromotionBatch{40};
  uint64_t minPromotionBatch{5};
};

} // namespace cachelib
} // namespace facebook

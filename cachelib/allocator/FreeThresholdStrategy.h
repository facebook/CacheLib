// @lint-ignore-every CLANGTIDY clang-diagnostic-unused-private-field

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

// Free threshold strategy for background promotion worker.
// This strategy tries to keep certain percent of memory free
// at all times.
class FreeThresholdStrategy : public BackgroundMoverStrategy {
 public:
  FreeThresholdStrategy(double lowEvictionAcWatermark,
                        double highEvictionAcWatermark,
                        uint64_t maxEvictionBatch,
                        uint64_t minEvictionBatch);
  ~FreeThresholdStrategy() {}

  std::vector<size_t> calculateBatchSizes(
      const CacheBase& cache, std::vector<MemoryDescriptorType> acVecs);

 private:
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif
  double lowEvictionAcWatermark{2.0};
  double highEvictionAcWatermark{5.0};
  uint64_t maxEvictionBatch{40};
  uint64_t minEvictionBatch{5};
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
};

} // namespace cachelib
} // namespace facebook

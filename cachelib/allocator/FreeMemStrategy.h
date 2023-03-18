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

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

// Strategy that frees a slab from any allocation class that's above the free
// memory limit. This strategy only picks the victim but not the receiver.
class FreeMemStrategy : public RebalanceStrategy {
 public:
  struct Config {
    // minimum number of slabs to retain in every allocation class.
    unsigned int minSlabs{1};

    // use free memory if it is amounts to more than this many slabs.
    unsigned int numFreeSlabs{3};

    // this strategy will not rebalance anything if the number
    // of free slabs is more than this number
    size_t maxUnAllocatedSlabs{1000};

    // free memory threshold to be used for picking victim.
    size_t getFreeMemThreshold() const noexcept {
      return numFreeSlabs * Slab::kSize;
    }

    Config() noexcept {}
    Config(unsigned int _minSlabs,
           unsigned int _numFreeSlabs,
           unsigned int _maxUnAllocatedSlabs) noexcept
        : minSlabs{_minSlabs},
          numFreeSlabs(_numFreeSlabs),
          maxUnAllocatedSlabs(_maxUnAllocatedSlabs) {}
  };

  explicit FreeMemStrategy(Config config = {});

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& cache,
                                             PoolId pid,
                                             const PoolStats& poolStats) final;

 private:
  const Config config_;
};
} // namespace cachelib
} // namespace facebook

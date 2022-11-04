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

#include "cachelib/allocator/PoolResizer.h"

#include <folly/logging/xlog.h>

#include "cachelib/allocator/PoolResizeStrategy.h"
#include "cachelib/common/Exceptions.h"

namespace facebook {
namespace cachelib {

PoolResizer::PoolResizer(CacheBase& cache,
                         unsigned int numSlabsPerIteration,
                         std::shared_ptr<RebalanceStrategy> strategy)
    : cache_(cache),
      strategy_(std::move(strategy)),
      numSlabsPerIteration_(numSlabsPerIteration) {
  if (!strategy_) {
    strategy_ = std::make_shared<PoolResizeStrategy>();
  }
}

PoolResizer::~PoolResizer() { stop(std::chrono::seconds(0)); }

void PoolResizer::work() {
  const auto pools = cache_.getRegularPoolIdsForResize();
  for (auto poolId : pools) {
    const PoolStats poolStats = cache_.getPoolStats(poolId);
    for (unsigned int i = 0; i < numSlabsPerIteration_; i++) {
      // check if the pool still needs resizing after each iteration.
      if (!cache_.getPool(poolId).overLimit()) {
        continue;
      }
      // if user had supplied a rebalance stategy for the pool,
      // use that to downsize it
      auto strategy = cache_.getResizeStrategy(poolId);
      if (!strategy) {
        strategy = strategy_;
      }

      // use the rebalance strategy and see if there is some allocation class
      // that is over provisioned.
      const auto classId = strategy->pickVictimForResizing(cache_, poolId);

      try {
        const auto now = util::getCurrentTimeMs();
        // Throws excption if the strategy did not pick a valid victim classId.
        cache_.releaseSlab(poolId, classId, SlabReleaseMode::kResize);
        XLOGF(DBG, "Moved a slab from classId {} for poolid: {}",
              static_cast<int>(classId), static_cast<int>(poolId));
        ++slabsReleased_;
        const auto elapsed_time =
            static_cast<uint64_t>(util::getCurrentTimeMs() - now);
        // Log the event about the Pool which released the Slab along with
        // the number of slabs. Only Victim Pool class information is
        // relevant here.
        stats_.addSlabReleaseEvent(
            classId, Slab::kInvalidClassId, /* No receiver Class info */
            elapsed_time, poolId, 1, 1,     /* One Slab moved */
            poolStats.allocSizeForClass(classId), 0,
            poolStats.evictionAgeForClass(classId), 0,
            poolStats.mpStats.acStats.at(classId).freeAllocs);
      } catch (const exception::SlabReleaseAborted& e) {
        XLOGF(WARN,
              "Aborted trying to resize pool {} for allocation class {}. "
              "Error: {}",
              static_cast<int>(poolId), static_cast<int>(classId), e.what());
        return;
      } catch (const std::exception& e) {
        XLOGF(
            CRITICAL,
            "Error trying to resize pool {} for allocation class {}. Error: {}",
            static_cast<int>(poolId), static_cast<int>(classId), e.what());
      }
    }
  }

  // compact cache resizing is heavy weight and involves resharding. do that
  // only when all the item pools are resized
  if (pools.empty()) {
    cache_.resizeCompactCaches();
  }
}
} // namespace cachelib
} // namespace facebook

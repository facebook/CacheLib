#include "cachelib/allocator/PoolRebalancer.h"

#include <folly/logging/xlog.h>

#include <stdexcept>
#include <thread>

namespace facebook {
namespace cachelib {

PoolRebalancer::PoolRebalancer(CacheBase& cache,
                               std::shared_ptr<RebalanceStrategy> strategy,
                               unsigned int freeAllocThreshold,
                               std::function<void()> postWorkHandler)
    : cache_(cache),
      defaultStrategy_(std::move(strategy)),
      freeAllocThreshold_(freeAllocThreshold),
      postWorkHandler_(std::move(postWorkHandler)) {
  if (!defaultStrategy_) {
    throw std::invalid_argument("The default rebalance strategy is not set.");
  }
}

PoolRebalancer::~PoolRebalancer() { stop(std::chrono::seconds(0)); }

void PoolRebalancer::work() {
  try {
    for (const auto pid : cache_.getRegularPoolIds()) {
      auto strategy = cache_.getRebalanceStrategy(pid);
      if (!strategy) {
        strategy = defaultStrategy_;
      }
      tryRebalancing(pid, *strategy);
    }
  } catch (const std::exception& ex) {
    XLOGF(ERR, "Rebalancing interrupted due to exception: {}", ex.what());
  }
}

void PoolRebalancer::releaseSlab(PoolId pid,
                                 ClassId victimClassId,
                                 ClassId receiverClassId) {
  const auto now = util::getCurrentTimeMs();

  cache_.releaseSlab(pid, victimClassId, receiverClassId,
                     SlabReleaseMode::kRebalance);
  const auto elapsed_time =
      static_cast<uint64_t>(util::getCurrentTimeMs() - now);
  const PoolStats poolStats = cache_.getPoolStats(pid);
  unsigned int numSlabsInReceiver = 0;
  uint32_t receiverAllocSize = 0;
  uint64_t receiverEvictionAge = 0;
  if (receiverClassId != Slab::kInvalidClassId) {
    numSlabsInReceiver = poolStats.numSlabsForClass(receiverClassId);
    receiverAllocSize = poolStats.allocSizeForClass(receiverClassId);
    receiverEvictionAge = poolStats.evictionAgeForClass(receiverClassId);
  }
  stats_.addSlabReleaseEvent(
      victimClassId, receiverClassId, elapsed_time, pid,
      poolStats.numSlabsForClass(victimClassId), numSlabsInReceiver,
      poolStats.allocSizeForClass(victimClassId), receiverAllocSize,
      poolStats.evictionAgeForClass(victimClassId), receiverEvictionAge,
      poolStats.mpStats.acStats.at(victimClassId).freeAllocs);
  XLOGF(DBG,
        "Moved slab in Pool Id: {}, Victim Class Id: {}, Receiver "
        "Class Id: {}",
        static_cast<int>(pid), static_cast<int>(victimClassId),
        static_cast<int>(receiverClassId));
}

RebalanceContext PoolRebalancer::pickVictimByFreeAlloc(PoolId pid) const {
  const auto& mpStats = cache_.getPool(pid).getStats();
  uint64_t maxFreeAllocSlabs = 1;
  ClassId retId = Slab::kInvalidClassId;
  for (auto& id : mpStats.classIds) {
    uint64_t freeAllocSlabs = mpStats.acStats.at(id).freeAllocs /
                              mpStats.acStats.at(id).allocsPerSlab;

    if (freeAllocSlabs > freeAllocThreshold_ &&
        freeAllocSlabs > maxFreeAllocSlabs) {
      maxFreeAllocSlabs = freeAllocSlabs;
      retId = id;
    }
  }
  RebalanceContext ctx;
  ctx.victimClassId = retId;
  ctx.receiverClassId = Slab::kInvalidClassId;
  return ctx;
}

bool PoolRebalancer::tryRebalancing(PoolId pid, RebalanceStrategy& strategy) {
  if (freeAllocThreshold_ > 0) {
    auto ctx = pickVictimByFreeAlloc(pid);
    if (ctx.victimClassId != Slab::kInvalidClassId) {
      releaseSlab(pid, ctx.victimClassId, Slab::kInvalidClassId);
    }
  }

  if (!cache_.getPool(pid).allSlabsAllocated()) {
    return false;
  }

  const auto context = strategy.pickVictimAndReceiver(cache_, pid);
  if (context.victimClassId == Slab::kInvalidClassId) {
    XLOGF(DBG,
          "Pool Id: {} rebalancing strategy didn't find an victim",
          static_cast<int>(pid));
    return false;
  }
  releaseSlab(pid, context.victimClassId, context.receiverClassId);
  return true;
}

} // namespace cachelib
} // namespace facebook

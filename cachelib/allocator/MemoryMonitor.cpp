
#include "cachelib/allocator/MemoryMonitor.h"

#include <folly/logging/xlog.h>

#include "cachelib/allocator/PoolResizeStrategy.h"
#include "cachelib/common/Exceptions.h"

namespace facebook {
namespace cachelib {

constexpr size_t kGBytes = 1024 * 1024 * 1024;

MemoryMonitor::MemoryMonitor(CacheBase& cache,
                             Mode mode,
                             size_t percentPerIteration,
                             size_t lowerLimitGB,
                             size_t upperLimitGB,
                             size_t maxLimitPercent,
                             std::shared_ptr<RebalanceStrategy> strategy,
                             std::function<void()> postWorkHandler)
    : cache_(cache),
      mode_(mode),
      strategy_(std::move(strategy)),
      percentPerIteration_(percentPerIteration),
      lowerLimit_(lowerLimitGB * kGBytes),
      upperLimit_(upperLimitGB * kGBytes),
      maxLimitPercent_(maxLimitPercent),
      postWorkHandler_(std::move(postWorkHandler)) {
  if (!strategy_) {
    strategy_ = std::make_shared<PoolResizeStrategy>();
  }
  // There should be at least a slab worth of difference between upper
  // and lower memory limits.
  XDCHECK_LT(lowerLimit_, upperLimit_ - Slab::kSize);
}

MemoryMonitor::~MemoryMonitor() {
  try {
    stop();
  } catch (const std::exception&) {
  }
}

void MemoryMonitor::work() {
  switch (mode_) {
  case FreeMemory:
    checkFreeMemory();
    break;
  case ResidentMemory:
    checkResidentMemory();
    break;
  case TestMode:
    checkPoolsAndAdviseReclaim();
    break;
  default:
    throw std::runtime_error("Unsupported memory monitoring mode");
  }
}

void MemoryMonitor::checkFreeMemory() {
  auto memFree = facebook::cachelib::util::getMemAvailable();
  const auto stats = cache_.getCacheMemoryStats();
  if (memFree < lowerLimit_) {
    XLOGF(DBG,
          "Free memory size of {} bytes is below the limit of {} bytes",
          memFree,
          lowerLimit_);
    adviseAwaySlabs();
  } else if (memFree > upperLimit_ && stats.numAdvisedSlabs() > 0) {
    XLOGF(DBG,
          "Free memory size of {} bytes is above the limit of {} bytes",
          memFree,
          upperLimit_);
    reclaimSlabs();
  }
  checkPoolsAndAdviseReclaim();
}

void MemoryMonitor::checkResidentMemory() {
  auto rss = static_cast<size_t>(facebook::cachelib::util::getRSSBytes());
  const auto stats = cache_.getCacheMemoryStats();
  if (rss > upperLimit_) {
    XLOGF(DBG,
          "Resident memory size of {} bytes is above the limit of {} bytes",
          rss,
          upperLimit_);
    adviseAwaySlabs();
  } else if (rss < lowerLimit_ && stats.numAdvisedSlabs() > 0) {
    XLOGF(DBG,
          "Resident memory size of {} bytes is below the limit of {} bytes",
          rss,
          lowerLimit_);
    reclaimSlabs();
  }
  checkPoolsAndAdviseReclaim();
}

namespace {
size_t bytesToSlabs(size_t bytes) { return bytes / Slab::kSize; }
} // namespace

size_t MemoryMonitor::getPoolUsedSlabs(PoolId poolId) const noexcept {
  return bytesToSlabs(cache_.getPool(poolId).getCurrentUsedSize());
}

size_t MemoryMonitor::getPoolSlabs(PoolId poolId) const noexcept {
  return bytesToSlabs(cache_.getPool(poolId).getPoolUsableSize());
}

size_t MemoryMonitor::getTotalSlabs() const noexcept {
  const auto pools = cache_.getRegularPoolIds();
  return std::accumulate(pools.begin(), pools.end(), 0ull,
                         [this](auto total, const auto& poolId) {
                           return total + getPoolSlabs(poolId);
                         });
}

size_t MemoryMonitor::getSlabsInUse() const noexcept {
  const auto pools = cache_.getRegularPoolIds();
  return std::accumulate(pools.begin(), pools.end(), 0ull,
                         [this](auto total, const auto& poolId) {
                           return total + getPoolUsedSlabs(poolId);
                         });
}

void MemoryMonitor::checkPoolsAndAdviseReclaim() {
  auto results = cache_.calcNumSlabsToAdviseReclaim();
  if (results.poolAdviseReclaimMap.empty()) {
    return;
  }
  // all result would either be advise or reclaim. It is not possible for
  // some of them to be advise and some to reclaim

  // Advise slabs, if marked for advise
  if (results.advise) {
    for (auto& result : results.poolAdviseReclaimMap) {
      uint64_t slabsAdvised = 0;
      PoolId poolId = result.first;
      uint64_t slabsToAdvise = result.second;
      while (slabsAdvised < slabsToAdvise) {
        const auto classId = strategy_->pickVictimForResizing(cache_, poolId);
        if (classId == Slab::kInvalidClassId) {
          break;
        }
        try {
          const auto now = util::getCurrentTimeMs();
          auto stats = cache_.getPoolStats(poolId);
          cache_.releaseSlab(poolId, classId, SlabReleaseMode::kAdvise);
          ++slabsAdvised;
          const auto elapsed_time =
              static_cast<uint64_t>(util::getCurrentTimeMs() - now);
          stats.numSlabsForClass(classId);
          stats.evictionAgeForClass(classId);
          // Log the event about the Pool which released the Slab along with
          // the number of slabs.
          stats_.addSlabReleaseEvent(
              classId, Slab::kInvalidClassId, /* No Class info */
              elapsed_time, poolId, stats.numSlabsForClass(classId),
              0 /* receiver slabs */, stats.allocSizeForClass(classId),
              0 /* receiver alloc size */, stats.evictionAgeForClass(classId),
              0 /* receiver eviction age */,
              stats.numFreeAllocsForClass(classId));

        } catch (const exception::SlabReleaseAborted& e) {
          XLOGF(WARN,
                "Aborted trying to advise away a slab from pool {} for"
                " allocation class {}. Error: {}",
                static_cast<int>(poolId), static_cast<int>(classId), e.what());
          return;
        } catch (const std::exception& e) {
          XLOGF(
              CRITICAL,
              "Error trying to advise away a slab from pool {} for allocation "
              "class {}. Error: {}",
              static_cast<int>(poolId), static_cast<int>(classId), e.what());
        }
      }
      slabsAdvised_ += slabsAdvised;
      XLOGF(DBG, "Advised away {} slabs from Pool ID: {}, to free {} bytes",
            slabsAdvised, static_cast<int>(poolId), slabsAdvised * Slab::kSize);
    }
    return;
  } else {
    XDCHECK(!results.advise);
    // Reclaim slabs, if marked for reclaim
    for (auto& result : results.poolAdviseReclaimMap) {
      PoolId poolId = result.first;
      uint64_t slabsToReclaim = result.second;
      auto slabsReclaimed = cache_.reclaimSlabs(poolId, slabsToReclaim);
      XLOGF(
          DBG,
          "Reclaimed {} of {} slabs for Pool ID: {}, to grow cache by {} bytes",
          slabsReclaimed, slabsToReclaim, static_cast<int>(poolId),
          slabsReclaimed * Slab::kSize);
      slabsReclaimed_ += slabsReclaimed;
    }
  }
}

void MemoryMonitor::adviseAwaySlabs() {
  const auto totalSlabsInUse = getSlabsInUse();
  const auto totalSlabs = getTotalSlabs();

  if (totalSlabsInUse == 0 || totalSlabs == 0) {
    // If there are no used slabs and we're still having to advise away, then
    // the cache size is too big!
    XLOG(DBG, "There are no slabs in use to advise away");
    return;
  }
  const auto numAdvised = cache_.getCacheMemoryStats().numAdvisedSlabs();
  const auto advisedPercent = numAdvised * 100 / (numAdvised + totalSlabs);
  if (advisedPercent > maxLimitPercent_) {
    XLOGF(CRITICAL,
          "More than {} slabs of {} ({}"
          "%) in the item cache memory have been advised away. "
          "This exceeds the maximum limit of {}"
          "%. Disabling advising which may result in an OOM.",
          numAdvised, totalSlabs, advisedPercent, maxLimitPercent_);
    return;
  }
  // Advise percentPerIteration_% of upperLimit_ - lowerLimit_ every iteration
  const auto slabsToAdvise =
      bytesToSlabs(upperLimit_ - lowerLimit_) * percentPerIteration_ / 100;
  XLOGF(DBG, "Advising away {} slabs to free {} bytes", slabsToAdvise,
        slabsToAdvise * Slab::kSize);
  cache_.updateNumSlabsToAdvise(slabsToAdvise);
}

void MemoryMonitor::reclaimSlabs() {
  // Reclaim N% of the total slabs every iteration
  auto slabsToReclaim =
      bytesToSlabs(upperLimit_ - lowerLimit_) * percentPerIteration_ / 100;
  const auto stats = cache_.getCacheMemoryStats();
  if (slabsToReclaim > stats.numAdvisedSlabs()) {
    slabsToReclaim = stats.numAdvisedSlabs();
  }
  if (slabsToReclaim == 0) {
    return;
  }

  const auto totalSlabsInUse = getSlabsInUse();
  if (totalSlabsInUse == 0) {
    XLOG(CRITICAL, "There are no slabs in use by items cache, cannot reclaim");
    return;
  }
  XLOGF(DBG, "Reclaiming {} slabs to increase cache size by {} bytes",
        slabsToReclaim, slabsToReclaim * Slab::kSize);
  cache_.updateNumSlabsToAdvise(-slabsToReclaim);
}

} // namespace cachelib
} // namespace facebook

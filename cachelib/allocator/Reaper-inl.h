#include "cachelib/allocator/memory/Slab.h"

namespace facebook {
namespace cachelib {

template <typename CacheT>
void Reaper<CacheT>::work() {
  if (waitUntilEvictions_) {
    const auto pools = ReaperAPIWrapper<CacheT>::getRegularPoolIds(cache_);
    uint64_t numEvictions = 0;
    for (PoolId pid : pools) {
      numEvictions += cache_.getPoolStats(pid).numEvictions();
    }
    if (numEvictions == 0) {
      return;
    }
  }

  slabWalkMode_ ? reapSlabWalkMode() : reapUsingIterator();
}

template <typename CacheT>
void Reaper<CacheT>::TraversalStats::recordTraversalTime(uint64_t msTaken) {
  lastTraversalTimeMs_.store(msTaken, std::memory_order_relaxed);
  minTraversalTimeMs_.store(std::min(minTraversalTimeMs_.load(), msTaken),
                            std::memory_order_relaxed);
  maxTraversalTimeMs_.store(std::max(minTraversalTimeMs_.load(), msTaken),
                            std::memory_order_relaxed);
  totalTraversalTimeMs_.fetch_add(msTaken, std::memory_order_relaxed);
}

template <typename CacheT>
uint64_t Reaper<CacheT>::TraversalStats::getAvgTraversalTimeMs(
    uint64_t numTraversals) const {
  return numTraversals ? totalTraversalTimeMs_ / numTraversals : 0;
}

template <typename CacheT>
void Reaper<CacheT>::reapUsingIterator() {
  // if we have completed an iteration, restart
  if (iter_ == cache_.end()) {
    auto curr = util::getCurrentTimeMs();
    traversalStats_.recordTraversalTime(
        curr > lastIterCreateTimeMs_ ? curr - lastIterCreateTimeMs_ : 0);
    lastIterCreateTimeMs_ = curr;
    iter_ = cache_.begin(throttlerConfig_);
  }

  // each work period, only check maxIteration_ number of items
  for (uint32_t i = 0; i < maxIteration_ && iter_ != cache_.end();
       ++i, ++iter_) {
    numVisitedItems_.fetch_add(1, std::memory_order_relaxed);
    const auto& handle = iter_.asHandle();
    // if iterator is expired, then remove it
    if (handle->isExpired() &&
        ReaperAPIWrapper<CacheT>::removeIfExpired(cache_, handle)) {
      numReapedItems_.fetch_add(1, std::memory_order_relaxed);
    }
  }
}

template <typename CacheT>
void Reaper<CacheT>::reapSlabWalkMode() {
  util::Throttler t(throttlerConfig_);
  const auto begin = util::getCurrentTimeMs();

  // use a local to accumulate counts since the lambda could be executed
  // millions of times per sec.
  uint64_t visits = 0;
  uint64_t reaps = 0;

  // unlike the iterator mode, in this mode, we traverse all the way
  ReaperAPIWrapper<CacheT>::traverseAndExpireItems(
      cache_, [&](void* ptr, facebook::cachelib::AllocInfo allocInfo) -> bool {
        XDCHECK(ptr);
        // see if we need to stop the traversal and accumulate counts to
        // global
        if (visits++ == kCheckThreshold) {
          numVisitedItems_.fetch_add(visits, std::memory_order_relaxed);
          numReapedItems_.fetch_add(reaps, std::memory_order_relaxed);
          visits = 0;
          reaps = 0;

          // abort the current iteration since we have to stop
          if (shouldStopWork()) {
            return false;
          }
        }

        // if we throttle, then we should check for stop condition after
        // the throttler has actually throttled us.
        if (t.throttle() && shouldStopWork()) {
          return false;
        }

        // get an item and check if it is expired and is in the access
        // container before we actually grab the
        // handle to the item and proceed to expire it.
        const auto& item = *reinterpret_cast<const Item*>(ptr);
        if (!item.isExpired() || !item.isAccessible()) {
          return true;
        }

        // Item has to be smaller than the alloc size to be a valid item.
        auto key = item.getKey();
        if (Item::getRequiredSize(key, 0 /* value size*/) >
            allocInfo.allocSize) {
          return true;
        }

        try {
          // obtain a valid handle without disturbing the state of the item in
          // cache.
          auto handle = cache_.peek(key);
          auto reaped =
              ReaperAPIWrapper<CacheT>::removeIfExpired(cache_, handle);
          if (reaped) {
            reaps++;
          }
        } catch (const std::exception& e) {
          numErrs_.fetch_add(1, std::memory_order_relaxed);
          XLOGF(DBG, "Error while reaping. Msg = {}", e.what());
        }
        return true;
      });

  // accumulate any left over visits, reaps.
  numVisitedItems_.fetch_add(visits, std::memory_order_relaxed);
  numReapedItems_.fetch_add(reaps, std::memory_order_relaxed);
  auto end = util::getCurrentTimeMs();
  traversalStats_.recordTraversalTime(end > begin ? end - begin : 0);
}

template <typename CacheT>
Reaper<CacheT>::Reaper(Cache& cache,
                       bool slabWalk,
                       const util::Throttler::Config& config,
                       uint32_t maxIteration,
                       bool waitUntilEvictions)
    : cache_(cache),
      slabWalkMode_(slabWalk),
      iter_(cache_.end()),
      throttlerConfig_(config),
      maxIteration_(maxIteration),
      waitUntilEvictions_(waitUntilEvictions) {}

template <typename CacheT>
Reaper<CacheT>::~Reaper() {
  stop(std::chrono::seconds(0));
}

template <typename CacheT>
ReaperStats Reaper<CacheT>::getStats() const noexcept {
  ReaperStats stats;
  stats.numVisitedItems = numVisitedItems_.load(std::memory_order_relaxed);
  stats.numReapedItems = numReapedItems_.load(std::memory_order_relaxed);
  stats.numVisitErrs = numErrs_.load(std::memory_order_relaxed);
  auto runCount = getRunCount();
  stats.numTraversals = runCount;
  stats.lastTraversalTimeMs = traversalStats_.getLastTraversalTimeMs();
  stats.avgTraversalTimeMs = traversalStats_.getAvgTraversalTimeMs(runCount);
  stats.minTraversalTimeMs = traversalStats_.getMinTraversalTimeMs();
  stats.maxTraversalTimeMs = traversalStats_.getMaxTraversalTimeMs();
  return stats;
}

} // namespace cachelib
} // namespace facebook

#pragma once

#include <limits>

#include "cachelib/allocator/CacheStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

// wrapper that exposes the private APIs of CacheType that are specifically
// needed for the Reaper.
template <typename C>
struct ReaperAPIWrapper {
  static std::set<PoolId> getRegularPoolIds(C& cache) {
    return cache.getRegularPoolIds();
  }

  static bool removeIfExpired(C& cache, const typename C::ItemHandle& handle) {
    return cache.removeIfExpired(handle);
  }

  template <typename Fn>
  static void traverseAndExpireItems(C& cache, Fn&& f) {
    cache.traverseAndExpireItems(std::forward<Fn>(f));
  }
};

// Remove the items that are expired in the cache. Creates a new thread
// for background checking with throttler to reap the expired items.
template <typename CacheT>
class Reaper : public PeriodicWorker {
 public:
  using Cache = CacheT;
  // this initialized an itemsReaper to check expired itemsReaper
  // @param cache               instance of the cache
  // @param config              throttler config during iteration
  // @param waitUntilEvictions  reaper shall wait until there're evictions
  Reaper(Cache& cache,
         const util::Throttler::Config& config,
         bool waitUntilEvictions);

  ~Reaper();

  ReaperStats getStats() const noexcept;

 private:
  struct TraversalStats {
    // record a traversal and its time taken
    void recordTraversalTime(uint64_t msTaken);

    uint64_t getAvgTraversalTimeMs(uint64_t numTraversals) const;
    uint64_t getMinTraversalTimeMs() const { return minTraversalTimeMs_; }
    uint64_t getMaxTraversalTimeMs() const { return maxTraversalTimeMs_; }
    uint64_t getLastTraversalTimeMs() const { return lastTraversalTimeMs_; }
    uint64_t getNumTraversals() const { return numTraversals_; }

   private:
    // time it took us the last time to traverse the cache.
    std::atomic<uint64_t> lastTraversalTimeMs_{0};
    std::atomic<uint64_t> minTraversalTimeMs_{
        std::numeric_limits<uint64_t>::max()};
    std::atomic<uint64_t> maxTraversalTimeMs_{0};
    std::atomic<uint64_t> totalTraversalTimeMs_{0};
    std::atomic<uint64_t> numTraversals_{0};
  };

  using Item = typename Cache::Item;

  // implement logic in the virtual function in PeriodicWorker
  // check whether the items is expired or not
  void work() override final;

  void reapSlabWalkMode();

  // reference to the cache
  Cache& cache_;

  const util::Throttler::Config throttlerConfig_;

  // By default, iterator will not reap until evictions happen
  const bool waitUntilEvictions_{false};

  TraversalStats traversalStats_;

  // stats on visited items
  std::atomic<uint64_t> numVisitedItems_{0};
  std::atomic<uint64_t> numReapedItems_{0};
  std::atomic<uint64_t> numErrs_{0};

  // number of items to visit before we check for stopping the worker in super
  // charged mode.
  static constexpr const uint64_t kCheckThreshold = 1ULL << 22;
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/Reaper-inl.h"

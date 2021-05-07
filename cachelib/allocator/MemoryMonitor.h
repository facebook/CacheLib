#pragma once

#include <atomic>

#include "cachelib/allocator/Cache.h"
#include "cachelib/allocator/RebalanceStrategy.h"
#include "cachelib/allocator/SlabReleaseStats.h"
#include "cachelib/common/PeriodicWorker.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class AllocatorResizeTest;
}

// The goal of memory monitoring is to avoid an out-of-memory (OOM) situation,
// either by ensuring there's enough free memory available on the system or
// that the caching process does not exceed a given memory usage limit.
// Note: For processes running inside cgroups with memory limits, the free
// memory monitoring does not work. Instead use the resident memory monitoring
// to keep the process memory usage below the cgroup memory limit.
class MemoryMonitor : public PeriodicWorker {
 public:
  enum Mode { FreeMemory, ResidentMemory, TestMode, Disabled };

  // Memory monitoring can be setup to run in one of the two following modes:
  //
  // 1. Free Memory Monitoring (Not supported for processes in cgroups)
  //
  // Setup a free memory monitor that periodically checks system free memory.
  // If it dips below lowerLimitGB bytes, it advises away percentPerIteration
  // percent of lowerLimitGB at a time (in every iteration), until system free
  // memory is above the lowerLimitGB bytes. If the system free memory exceeds
  // upperLimitGB bytes, it reclaims percentPerIteration percent of lowerLimitGB
  // at a time until system free memory drops below upperLimitGB. A maximum of
  // maxLimitPercent of total cache size (excluding compact cache) can be
  // advised away, after which advising stops to avoid cache from becoming
  // too small.
  // Given N bytes of memory on a host, typically N-M bytes are used by cache
  // leaving M bytes for the heap usage by cache process, kernel and other
  // processes running on the box. When non-cache memory usage exceeds M bytes
  // the host goes Out-Of-Memory (OOM) and may fail or kill the cache process.
  // The free memory monitor ensures that there's at least lowerLimitGB amount
  // of memory free by giving up to maxLimitPercent of the cache (excluding
  // compact cache), there by avoiding OOM condition.
  //
  // @param mode                 FreeMemory
  // @param cache                Cache
  // @param percentPerIteration  Percentage of upperLimitGB-lowerLimitGB to be
  //                             advised or reclaimed every poll period. This
  //                             governs the rate of advise/reclaim.
  // @param lowerLimitGB         The lower limit of free memory in GBytes that
  //                             triggers advising away of memory from cache
  // @param upperLimitGB         The upper limit of free memory in GBytes that
  //                             triggers reclaiming of advised away memory
  // @param maxLimitPercent      Maximum percentage of item cache limit that can
  //                             be advised away before advising is disabled
  //                             leading to a probable OOM.
  // @param strategy             Strategy to use to determine the allocation
  //                             class in pool to steal slabs from, for advising
  // @param postWorkHandler      Function invoked after periodic poll completes
  //
  // 2. Resident Memory Monitoring
  //
  // Setup a resident memory monitor to advise away memory to avoid OOM, by
  // by limiting process's total resident memory usage. The resident memory
  // usage of the process can be split into two parts, the cache and everything
  // else. When the resident memory usage exceeds the upperLimitGB, the monitor
  // gives away memory from cache, percentPerIteration of upperLimitGB every
  // poll period, until the memory usage drops below upperLimitGB. When the
  // resident memory usage dips below lowerLimitGB, the monitor reclaims memory
  // for cache (if previously given away), until the resident memory usage is
  // above the lowerLimitGB, percentPerIteration of upperLimitGB at a time.
  //
  // @param mode                 ResidentMemory
  // @param cache
  // @param percentPerIteration  Percentage of upperLimitGB-lowerLimitGB to be
  //                             advised or reclaimed every poll period. This
  //                             governs the rate of advise/reclaim.
  // @param lowerLimitGB         The lower limit of resident memory in GBytes
  //                             that triggers reclaiming of previously advised
  //                             away of memory from cache
  // @param upperLimitGB         The upper limit of resident memory in GBytes
  //                             that triggers advising of memory from cache
  // @param maxLimitPercent      Maximum percentage of item cache limit that can
  //                             be advised away before advising is disabled
  //                             leading to a probable OOM.
  // @param strategy             Strategy to use to determine the allocation
  //                             class in pool to steal slabs from, for advising
  // @param postWorkHandler      Function invoked after periodic poll completes
  MemoryMonitor(CacheBase& cache,
                Mode mode,
                size_t percentPerIteration,
                size_t lowerLimitGB,
                size_t upperLimitGB,
                size_t maxLimitPercent,
                std::shared_ptr<RebalanceStrategy> strategy,
                std::function<void()> postWorkHandler = {});

  ~MemoryMonitor() override;

  // number of slabs that have been advised away
  unsigned int getNumSlabsAdvisedAway() const noexcept { return slabsAdvised_; }

  // number of slabs that have been reclaimed
  unsigned int getNumSlabsReclaimed() const noexcept { return slabsReclaimed_; }

  // maximum percentage of regular cache memory that can be advised away.
  double getMaxAdvisePct() const noexcept { return maxLimitPercent_; }

  // amount of memory available on the host
  size_t getMemAvailableSize() const noexcept { return memAvailableSize_; }

  // rss size of the process
  size_t getMemRssSize() const noexcept { return memRssSize_; }

  SlabReleaseEvents getSlabReleaseEvents(PoolId pid) const {
    return stats_.getSlabReleaseEvents(pid);
  }

 private:
  // check free memory and advise/reclaim if necessary
  void checkFreeMemory();

  // check resident memory and advise/reclaim if necessary
  void checkResidentMemory();

  // check pools for memory to be advised or reclaimed and execute
  // Checks the target number of slabs to be advised and compares with
  // the currently advised away slabs. Slabs are advised away or reclaimed
  // to make these two numbers equal
  void checkPoolsAndAdviseReclaim();

  // @param poolId the pool id
  // @return number of slabs in use by pool
  size_t getPoolUsedSlabs(PoolId poolId) const noexcept;

  // @param poolId the pool id
  // @return number of slabs in based on pool size
  size_t getPoolSlabs(PoolId poolId) const noexcept;

  // @return number of slabs based on the size of all pools added together
  size_t getTotalSlabs() const noexcept;

  // @return number of slabs in use by all pools together
  size_t getSlabsInUse() const noexcept;

  // advise away (percentPerIteration_ percent of lowerLimit_)  of slabs to
  // increase free memory
  void adviseAwaySlabs();

  // reclaim slabs (percentPerIteration_ percent of lowerLimit_) of slabs to
  // increase cache size
  void reclaimSlabs();

  // cache's interface for rebalancing
  CacheBase& cache_;

  // Memory monitoring mode
  Mode mode_;

  // user-defined rebalance strategy that would be used to pick a victim. If
  // this does not work out, we pick the first allocation class that has
  // non-zero slabs.
  std::shared_ptr<RebalanceStrategy> strategy_;

  // slab release stats for memory monitor.
  ReleaseStats stats_{};

  // number of slabs released as a part of resizing pools.
  std::atomic<unsigned int> slabsReleased_{0};

  // Specifies the percentage of the lower limit that is advised away or
  // relcaimed in an iteration of the memory monitor.
  size_t percentPerIteration_{0};

  // lower limit for free/resident memory in GBs.
  // Note: the lower/upper limit is used in exactly opposite ways for the
  // FreeMemory versus ResidentMemory mode.
  // 1. In the ResidentMemory mode, when the resident memory usage drops
  // below this limit, advised away slabs are reclaimed in proportion to
  // the size of pools, to increase cache size and raise resident memory
  // above this limit.
  // 2. In the FreeMemory mode, when the system free memory drops below this
  // limit, slabs are advised away from pools in proportion to their size to
  // raise system free memory above this limit.
  size_t lowerLimit_{0};

  // upper limit for free/resident memory in GBs.
  // Note: the lower/upper limit is used in exactly opposite ways for the
  // FreeMemory versus ResidentMemory mode.
  // 1. In the ResidentMemory mode, when the resident memory usage exceeds
  // this limit, slabs are advised away from pools in proportion to their
  // size to reduce resident memory usage below this limit.
  // 2. In the FreeMemory mode, when the system free memory exceeds
  // this limit and if there are slabs that were advised away earlier,
  // they're reclaimed by pools in proportion to their sizes to reduce the
  // system free memory below this limit.
  size_t upperLimit_{0};

  // the maximum percentage of total memory that can be advised away
  size_t maxLimitPercent_{0};

  // user defined handler that will be executed when the resizer stops
  std::function<void()> postWorkHandler_;

  // a count of total number of slabs advised away
  std::atomic<unsigned int> slabsAdvised_{0};

  template <typename AllocatorT>
  friend class facebook::cachelib::tests::AllocatorResizeTest;

  // a count of total number of slabs reclaimed
  std::atomic<unsigned int> slabsReclaimed_{0};

  // amount of memory available on the host
  std::atomic<size_t> memAvailableSize_{0};
  // rss size of the process
  std::atomic<size_t> memRssSize_{0};

  // implements the actual logic of running tryRebalancing and
  // updating the stats
  void work() final;

  // executes a user-defined postWork handler
  void postWork() final {
    if (postWorkHandler_) {
      postWorkHandler_();
    }
  }
};
} // namespace cachelib
} // namespace facebook

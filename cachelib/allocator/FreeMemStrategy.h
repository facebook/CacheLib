#pragma once

#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {

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
                                             PoolId pid) final;

 private:
  const Config config_;
};
}
}

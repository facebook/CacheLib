#pragma once

#include <chrono>
#include <memory>

#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  // Add a new region for tracking
  virtual void track(RegionId id) = 0;

  // Touch (record a hit) this region
  virtual void touch(RegionId id) = 0;

  // Evict a region and stop tracking
  virtual RegionId evict() = 0;

  // Reset policy to the initial state
  virtual void reset() = 0;

  // Memory used by the policy
  virtual size_t memorySize() const = 0;

  virtual void getCounters(const CounterVisitor&) const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

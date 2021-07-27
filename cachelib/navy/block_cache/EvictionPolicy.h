#pragma once

#include <chrono>
#include <memory>

#include "cachelib/navy/block_cache/Region.h"
#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Abstract base class of an eviction policy.
class EvictionPolicy {
 public:
  virtual ~EvictionPolicy() = default;

  // Adds a new region for tracking.
  // @param region    region to be tracked
  virtual void track(const Region& region) = 0;

  // Touches (record a hit) this region.
  virtual void touch(RegionId id) = 0;

  // Evicts a region and stops tracking.
  virtual RegionId evict() = 0;

  // Resets policy to the initial state.
  virtual void reset() = 0;

  // Gets memory used by the policy.
  virtual size_t memorySize() const = 0;

  // Exports policy stats via CounterVisitor.
  virtual void getCounters(const CounterVisitor&) const = 0;

  // Persists metadata associated with this policy.
  virtual void persist(RecordWriter& rw) const = 0;

  // Recovers from previously persisted metadata associated with this policy.
  virtual void recover(RecordReader& rr) = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

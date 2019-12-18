#pragma once

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
class AdmissionPolicy {
 public:
  virtual ~AdmissionPolicy() = default;

  // Returns false if insert should be ignored
  virtual bool accept(HashedKey hk, BufferView value) = 0;

  // Reset policy to the initial state
  virtual void reset() = 0;

  // Get policy specific counters. Calls back @visitor with key name and value.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

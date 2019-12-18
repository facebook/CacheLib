#pragma once

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
class ReinsertionPolicy {
 public:
  virtual ~ReinsertionPolicy() = default;

  // Start tracking a key for reinsertion.
  virtual void track(HashedKey hk) = 0;

  // Touch an item every time we access it. User must track
  // an item before touching it. Touching without tracking
  // results in no-op.
  virtual void touch(HashedKey hk) = 0;

  // Determine whether or not we should keep this key around longer in cache.
  virtual bool shouldReinsert(HashedKey hk) = 0;

  // Remove a key from tracking. If key not found, no-op.
  virtual void remove(HashedKey hk) = 0;

  // Reset the state of this policy to when it was first initialized.
  virtual void reset() = 0;

  // Persist metadata associated with this policy.
  virtual void persist(RecordWriter& rw) = 0;

  // Recover from previously persisted metadata associated with this policy.
  virtual void recover(RecordReader& rr) = 0;

  // Export stats
  virtual void getCounters(const CounterVisitor& visitor) const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

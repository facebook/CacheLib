#pragma once

#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
class ReinsertionPolicy {
 public:
  virtual ~ReinsertionPolicy() = default;

  // pass in the index
  virtual void setIndex(Index* index) = 0;

  // Determine whether or not we should keep this key around longer in cache.
  virtual bool shouldReinsert(HashedKey hk) = 0;

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

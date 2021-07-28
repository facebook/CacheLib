#pragma once

#include "cachelib/navy/block_cache/Index.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Abstract base class of a reinsertion policy.
class ReinsertionPolicy {
 public:
  virtual ~ReinsertionPolicy() = default;

  // Passes in the index.
  virtual void setIndex(Index* index) = 0;

  // Determines whether or not we should keep this key around longer in cache.
  virtual bool shouldReinsert(HashedKey hk) = 0;

  // Persists metadata associated with this policy.
  virtual void persist(RecordWriter& rw) = 0;

  // Recovers from previously persisted metadata associated with this policy.
  virtual void recover(RecordReader& rr) = 0;

  // Exports policy stats via CounterVisitor.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

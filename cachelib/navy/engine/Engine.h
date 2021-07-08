#pragma once

#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
class Engine {
 public:
  virtual ~Engine() = default;

  // returns true if the entry could exist. False if the entry definitely does
  // not exist.
  virtual bool couldExist(HashedKey hk) = 0;

  // If insert is failed, previous item (if existed) is not affected and
  // remains available via lookup.
  virtual Status insert(HashedKey hk, BufferView value) = 0;

  virtual Status lookup(HashedKey hk, Buffer& value) = 0;

  // Remove must not return Status::Retry.
  virtual Status remove(HashedKey hk) = 0;

  // Flush all buffered (in flight) operations
  virtual void flush() = 0;

  // Reset cache to the initial state
  virtual void reset() = 0;

  virtual void persist(RecordWriter& rw) = 0;
  virtual bool recover(RecordReader& rr) = 0;

  // Get engine specific counters. Calls back @visitor with key name and value.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;

  // Get the maximum item size that can be inserted into the engine.
  virtual uint64_t getMaxItemSize() const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

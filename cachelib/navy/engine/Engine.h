#pragma once

#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Abstract base class of an engine.
class Engine {
 public:
  virtual ~Engine() = default;

  // Returns true if the entry could exist. False if the entry definitely does
  // not exist.
  virtual bool couldExist(HashedKey hk) = 0;

  // If insert is failed, previous item (if existed) is not affected and
  // remains available via lookup.
  virtual Status insert(HashedKey hk, BufferView value) = 0;

  // Looks up a key in the engine.
  virtual Status lookup(HashedKey hk, Buffer& value) = 0;

  // Remove must not return Status::Retry.
  virtual Status remove(HashedKey hk) = 0;

  // Flushes all buffered (in flight) operations
  virtual void flush() = 0;

  // Resets cache to the initial state
  virtual void reset() = 0;

  // Serializes engine state to a RecordWriter.
  virtual void persist(RecordWriter& rw) = 0;

  // Deserialize engine state from a RecordReader.
  //
  // @return  true if recovery succeeds, false otherwise.
  virtual bool recover(RecordReader& rr) = 0;

  // Gets engine specific counters. Calls back @visitor with key name and value.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;

  // Gets the maximum item size that can be inserted into the engine.
  virtual uint64_t getMaxItemSize() const = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

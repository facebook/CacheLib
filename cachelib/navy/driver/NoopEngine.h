#pragma once

#include "cachelib/navy/engine/Engine.h"

namespace facebook {
namespace cachelib {
namespace navy {
class NoopEngine final : public Engine {
 public:
  ~NoopEngine() override = default;
  Status insert(HashedKey /* hk */,
                BufferView /* value */,
                InsertOptions /* opt */) override {
    return Status::Rejected;
  }
  Status lookup(HashedKey /* hk */, Buffer& /* value */) override {
    return Status::NotFound;
  }
  Status remove(HashedKey /* hk */) override { return Status::NotFound; }
  void flush() override {}
  void reset() override {}
  void persist(RecordWriter& /* rw */) override {}
  bool recover(RecordReader& /* rr */) override { return true; }
  void getCounters(const CounterVisitor& /* visitor */) const override {}
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

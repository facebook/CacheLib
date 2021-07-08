#pragma once

#include "cachelib/navy/engine/Engine.h"

namespace facebook {
namespace cachelib {
namespace navy {
class NoopEngine final : public Engine {
 public:
  ~NoopEngine() override = default;
  Status insert(HashedKey /* hk */, BufferView /* value */) override {
    return Status::Rejected;
  }
  bool couldExist(HashedKey) override { return false; }
  Status lookup(HashedKey /* hk */, Buffer& /* value */) override {
    return Status::NotFound;
  }
  Status remove(HashedKey /* hk */) override { return Status::NotFound; }
  void flush() override {}
  void reset() override {}
  void persist(RecordWriter& /* rw */) override {}
  bool recover(RecordReader& /* rr */) override { return true; }
  void getCounters(const CounterVisitor& /* visitor */) const override {}
  uint64_t getMaxItemSize() const override { return UINT32_MAX; }
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

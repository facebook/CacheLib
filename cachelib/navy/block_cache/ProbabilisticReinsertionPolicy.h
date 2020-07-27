#pragma once

#include <cstdint>

#include <folly/Random.h>

#include "cachelib/navy/block_cache/ReinsertionPolicy.h"

namespace facebook {
namespace cachelib {
namespace navy {
class ProbabilisticReinsertionPolicy : public ReinsertionPolicy {
 public:
  // @param probability     reinsertion chances: 0 - 100
  explicit ProbabilisticReinsertionPolicy(uint32_t probability)
      : probability_{probability} {}

  void setIndex(Index* /* index */) override {}

  void touch(HashedKey /* hk */) override {}

  bool shouldReinsert(HashedKey /* hk */) override {
    return folly::Random::rand32() % 100 < probability_;
  }

  void remove(HashedKey /* hk */) override {}

  void reset() override {}

  void persist(RecordWriter& /* rw */) override {}

  void recover(RecordReader& /* rr */) override {}

  void getCounters(const CounterVisitor& /* visitor */) const override {}

 private:
  const uint32_t probability_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

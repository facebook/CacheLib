#pragma once

#include <folly/Random.h>

#include <cstdint>

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

  bool shouldReinsert(HashedKey /* hk */) override {
    return folly::Random::rand32() % 100 < probability_;
  }

  void persist(RecordWriter& /* rw */) override {}

  void recover(RecordReader& /* rr */) override {}

  void getCounters(const CounterVisitor& /* visitor */) const override {}

 private:
  const uint32_t probability_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

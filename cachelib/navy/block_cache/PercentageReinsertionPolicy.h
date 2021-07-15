#pragma once

#include <folly/Random.h>

#include <cstdint>

#include "cachelib/navy/block_cache/ReinsertionPolicy.h"
#include "folly/logging/xlog.h"

namespace facebook {
namespace cachelib {
namespace navy {
class PercentageReinsertionPolicy : public ReinsertionPolicy {
 public:
  // @param percentage     reinsertion chances: 0 - 100
  explicit PercentageReinsertionPolicy(uint32_t percentage)
      : percentage_{percentage} {
    XDCHECK(percentage > 0 && percentage <= 100);
  }

  void setIndex(Index* /* index */) override {}

  bool shouldReinsert(HashedKey /* hk */) override {
    return folly::Random::rand32() % 100 < percentage_;
  }

  void persist(RecordWriter& /* rw */) override {}

  void recover(RecordReader& /* rr */) override {}

  void getCounters(const CounterVisitor& /* visitor */) const override {}

 private:
  const uint32_t percentage_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

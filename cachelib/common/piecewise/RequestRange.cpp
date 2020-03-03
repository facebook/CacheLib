#include "cachelib/common/piecewise/RequestRange.h"

#include <folly/String.h>

namespace facebook {
namespace cachelib {

RequestRange::RequestRange(folly::Optional<RangePair> range) : range_(range) {}

RequestRange::RequestRange(folly::Optional<uint64_t> rangeStart,
                           folly::Optional<uint64_t> rangeEnd) {
  if (!rangeStart) {
    return;
  }

  if (rangeStart && rangeEnd && *rangeEnd < *rangeStart) {
    return;
  }

  range_ = std::make_pair(*rangeStart, rangeEnd);
}

} // namespace cachelib
} // namespace facebook

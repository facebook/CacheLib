#pragma once

#include <folly/Optional.h>

#include <string>

namespace facebook {
namespace cachelib {

/**
 * The class defines the range request index, this is used in situation like
 * HTTP range request. The class can be constructed by either specifying the
 * range start and end byte, or providing the range directly.
 *
 */
class RequestRange {
 public:
  using RangePair = std::pair<uint64_t, folly::Optional<uint64_t>>;

  explicit RequestRange(folly::Optional<RangePair> range);

  explicit RequestRange(folly::Optional<uint64_t> rangeStart,
                        folly::Optional<uint64_t> rangeEnd);

  /**
   * Get the actual bytes indexes that we need to return for the request.
   * Returns folly::none if no or invalid range provided.
   */
  const folly::Optional<RangePair> getRequestRange() const { return range_; }

 protected:
  folly::Optional<RangePair> range_{folly::none};
};

} // namespace cachelib
} // namespace facebook

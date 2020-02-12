
#include <cmath>
#include <limits>

#include <folly/Format.h>

#include "cachelib/common/CountMinSketch.h"
#include "cachelib/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace util {
CountMinSketch::CountMinSketch(double error,
                               double probability,
                               uint32_t maxWidth,
                               uint32_t maxDepth)
    : CountMinSketch{calculateWidth(error, maxWidth),
                     calculateDepth(probability, maxDepth)} {}

CountMinSketch::CountMinSketch(uint32_t width, uint32_t depth)
    : width_{width}, depth_{depth} {
  if (width_ == 0) {
    throw std::invalid_argument{
        folly::sformat("Width must be greater than 0. Width: {}", width)};
  }

  if (depth_ == 0) {
    throw std::invalid_argument{
        folly::sformat("Depth must be greater than 0. Depth: {}", depth)};
  }

  table_ = std::make_unique<uint32_t[]>(width_ * depth_);
  reset();
}

uint32_t CountMinSketch::calculateWidth(double error, uint32_t maxWidth) {
  if (error <= 0 || error >= 1) {
    throw std::invalid_argument{folly::sformat(
        "Error should be greater than 0 and less than 1. Error: {}", error)};
  }

  // From "Approximating Data with the Count-Min Data Structure" (Cormode &
  // Muthukrishnan)
  uint32_t width = std::ceil(2 / error);
  if (maxWidth > 0) {
    width = std::min(maxWidth, width);
  }
  return width;
}

uint32_t CountMinSketch::calculateDepth(double probability, uint32_t maxDepth) {
  if (probability <= 0 || probability >= 1) {
    throw std::invalid_argument{folly::sformat(
        "Probability should be greater than 0 and less than 1. Probability: {}",
        probability)};
  }

  // From "Approximating Data with the Count-Min Data Structure" (Cormode &
  // Muthukrishnan)
  uint32_t depth = std::ceil(std::log(1 - probability) / std::log(0.5));
  depth = std::max(1u, depth);
  if (maxDepth > 0) {
    depth = std::min(maxDepth, depth);
  }
  return depth;
}

void CountMinSketch::increment(uint64_t key) {
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    table_[index] += 1;
  }
}

uint32_t CountMinSketch::getCount(uint64_t key) const {
  auto count = std::numeric_limits<uint32_t>::max();
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    count = std::min(count, table_[index]);
  }
  return count * (depth_ != 0);
}

void CountMinSketch::resetCount(uint64_t key) {
  auto count = getCount(key);
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    table_[index] -= count;
  }
}

void CountMinSketch::decayCountsBy(double decay) {
  // Delete previous table and reinitialize
  uint64_t tableSize = width_ * depth_;
  for (uint64_t i = 0; i < tableSize; i++) {
    table_[i] *= decay;
  }
}

uint64_t CountMinSketch::getIndex(uint32_t hashNum, uint64_t key) const {
  auto rowIndex = facebook::cachelib::combineHashes(
                      facebook::cachelib::hashInt(hashNum), key) %
                  width_;
  return hashNum * width_ + rowIndex;
}
} // namespace util
} // namespace cachelib
} // namespace facebook

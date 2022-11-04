/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <folly/Format.h>

#include <cmath>
#include <limits>

#include "cachelib/common/Hash.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace util {
namespace detail {
template <typename UINT>
CountMinSketchBase<UINT>::CountMinSketchBase(double error,
                                             double probability,
                                             uint32_t maxWidth,
                                             uint32_t maxDepth)
    : CountMinSketchBase{calculateWidth(error, maxWidth),
                         calculateDepth(probability, maxDepth)} {}

template <typename UINT>
CountMinSketchBase<UINT>::CountMinSketchBase(uint32_t width, uint32_t depth)
    : width_{width}, depth_{depth} {
  if (width_ == 0) {
    throw std::invalid_argument{
        folly::sformat("Width must be greater than 0. Width: {}", width)};
  }

  if (depth_ == 0) {
    throw std::invalid_argument{
        folly::sformat("Depth must be greater than 0. Depth: {}", depth)};
  }

  table_ = std::make_unique<UINT[]>(width_ * depth_);
  reset();
}

template <typename UINT>
uint32_t CountMinSketchBase<UINT>::calculateWidth(double error,
                                                  uint32_t maxWidth) {
  if (error <= 0 || error >= 1) {
    throw std::invalid_argument{folly::sformat(
        "Error should be greater than 0 and less than 1. Error: {}", error)};
  }

  // From "Approximating Data with the Count-Min Data Structure" (Cormode &
  // Muthukrishnan)
  uint32_t width = narrow_cast<uint32_t>(std::ceil(2 / error));
  if (maxWidth > 0) {
    width = std::min(maxWidth, width);
  }
  return width;
}

template <typename UINT>
uint32_t CountMinSketchBase<UINT>::calculateDepth(double probability,
                                                  uint32_t maxDepth) {
  if (probability <= 0 || probability >= 1) {
    throw std::invalid_argument{folly::sformat(
        "Probability should be greater than 0 and less than 1. Probability: {}",
        probability)};
  }

  // From "Approximating Data with the Count-Min Data Structure" (Cormode &
  // Muthukrishnan)
  uint32_t depth = narrow_cast<uint32_t>(
      std::ceil(std::abs(std::log(1 - probability) / std::log(2))));
  depth = std::max(1u, depth);
  if (maxDepth > 0) {
    depth = std::min(maxDepth, depth);
  }
  return depth;
}

template <typename UINT>
void CountMinSketchBase<UINT>::increment(uint64_t key) {
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    if (table_[index] < getMaxCount()) {
      table_[index] += 1;
      if (table_[index] == getMaxCount()) {
        saturated += 1;
      }
    }
  }
}

template <typename UINT>
UINT CountMinSketchBase<UINT>::getCount(uint64_t key) const {
  UINT count = getMaxCount();
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    count = std::min(count, table_[index]);
  }
  return count * (depth_ != 0);
}

template <typename UINT>
void CountMinSketchBase<UINT>::resetCount(uint64_t key) {
  auto count = getCount(key);
  for (uint32_t hashNum = 0; hashNum < depth_; hashNum++) {
    auto index = getIndex(hashNum, key);
    table_[index] -= count;
  }
}

template <typename UINT>
void CountMinSketchBase<UINT>::decayCountsBy(double decay) {
  // Delete previous table and reinitialize
  uint64_t tableSize = width_ * depth_;
  for (uint64_t i = 0; i < tableSize; i++) {
    table_[i] = narrow_cast<UINT>(table_[i] * decay);
  }
}

template <typename UINT>
uint64_t CountMinSketchBase<UINT>::getIndex(uint32_t hashNum,
                                            uint64_t key) const {
  auto rowIndex = facebook::cachelib::combineHashes(
                      facebook::cachelib::hashInt(hashNum), key) %
                  width_;
  return hashNum * width_ + rowIndex;
}
} // namespace detail
} // namespace util
} // namespace cachelib
} // namespace facebook

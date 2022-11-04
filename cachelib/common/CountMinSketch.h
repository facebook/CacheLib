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

#pragma once

#include <memory>
#include <stdexcept>

namespace facebook {
namespace cachelib {
namespace util {
namespace detail {
// A probabilistic counting data structure that never undercounts items before
// it hits counter's capacity. It is a table structure with the depth being the
// number of hashes and the width being the number of unique items. When a key
// is inserted, each row's hash function is used to generate the index for that
// row. Then the element's count at that index is incremented. As a result, one
// key being inserted will increment different indicies in each row. Querying
// the count returns the minimum values of these elements since some hashes
// might collide.
//
// Users are supposed to synchronize concurrent accesses to the data
// structure.
//
// E.g. insert(1)
// hash1(1) = 2 -> increment row 1, index 2
// hash2(1) = 5 -> increment row 2, index 5
// hash3(1) = 3 -> increment row 3, index 3
// etc.

// Taking counter type as parameter. Increments after the counter hitting its
// capacity would be ignored.
// Using a smaller counter type would reduce memory footprint.
template <typename UINT>
class CountMinSketchBase {
 public:
  // @param errors        Tolerable error in count given as a fraction of the
  //                      total number of inserts. Must be between 0 and 1.
  // @param probability   The certainty that the count is within the
  //                      error threshold. Must be between 0 and 1.
  // @param maxWidth      Maximum number of elements per row in the table.
  // @param maxDepth      Maximum number of rows.
  // Throws std::exception.
  CountMinSketchBase(double error,
                     double probability,
                     uint32_t maxWidth,
                     uint32_t maxDepth);

  CountMinSketchBase(uint32_t width, uint32_t depth);
  CountMinSketchBase() = default;

  CountMinSketchBase(const CountMinSketchBase&) = delete;
  CountMinSketchBase& operator=(const CountMinSketchBase&) = delete;

  CountMinSketchBase(CountMinSketchBase&& other) noexcept
      : width_(other.width_),
        depth_(other.depth_),
        table_(std::move(other.table_)) {
    other.width_ = 0;
    other.depth_ = 0;
  }

  CountMinSketchBase& operator=(CountMinSketchBase&& other) {
    if (this != &other) {
      this->~CountMinSketchBase();
      new (this) CountMinSketchBase(std::move(other));
    }
    return *this;
  }

  UINT getCount(uint64_t key) const;
  void increment(uint64_t key);
  void resetCount(uint64_t key);

  // decays all counts by the given decay rate. count *= decay
  void decayCountsBy(double decay);

  // Sets count for all keys to zero
  void reset() {
    uint64_t tableSize = width_ * depth_;
    for (uint64_t i = 0; i < tableSize; i++) {
      table_[i] = 0;
    }
  }

  uint32_t width() const { return width_; }

  uint32_t depth() const { return depth_; }

  uint64_t getByteSize() const { return width_ * depth_ * sizeof(UINT); }

  UINT getMaxCount() const { return std::numeric_limits<UINT>::max(); }

  // Get the number of saturated cells.
  uint64_t getSaturatedCounts() { return saturated; }

 private:
  static uint32_t calculateWidth(double error, uint32_t maxWidth);
  static uint32_t calculateDepth(double probability, uint32_t maxDepth);

  // Get the index for @hashNumber row in the table
  uint64_t getIndex(uint32_t hashNumber, uint64_t key) const;

  uint32_t width_{0};
  uint32_t depth_{0};
  uint64_t saturated{0};

  // Stores counts
  std::unique_ptr<UINT[]> table_{};
};
} // namespace detail

// By default, use uint32_t as count type.
using CountMinSketch = detail::CountMinSketchBase<uint32_t>;
using CountMinSketch8 = detail::CountMinSketchBase<uint8_t>;
using CountMinSketch16 = detail::CountMinSketchBase<uint16_t>;

} // namespace util
} // namespace cachelib
} // namespace facebook

#include "cachelib/common/CountMinSketch-inl.h"

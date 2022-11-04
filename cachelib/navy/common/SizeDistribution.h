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

#include <folly/logging/xlog.h>

#include <atomic>
#include <cstdint>
#include <map>
#include <vector>

#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {
namespace navy {
// This is used to track distribution of a set of sizes. This class supports
// both adding and removing sizes from the distribution.
class SizeDistribution {
 public:
  // Create a size distribution that spans [@min, @max] at a granularity of
  // @factor.
  SizeDistribution(uint64_t min, uint64_t max, double factor);

  // Recover from a previously saved snapshot
  explicit SizeDistribution(std::map<int64_t, int64_t> snapshot);

  // Add a new sample of "size" to the distribution
  // @size    size should be between [min, max] specified at construction
  void addSize(uint64_t size);

  // Remove a sample of "size" to the distribution
  // @size    size should be between [min, max] specified at construction
  void removeSize(uint64_t size);

  // Return {size -> number of items} mapping
  // Return signed value so it's easy to use this with thrift structures
  std::map<int64_t, int64_t> getSnapshot() const;

  // Clear all samples
  void reset();

 private:
  std::map<uint64_t, AtomicCounter> dist_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

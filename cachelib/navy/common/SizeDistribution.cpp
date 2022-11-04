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

#include "cachelib/navy/common/SizeDistribution.h"

#include <algorithm>
#include <cassert>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
std::vector<uint64_t> generateSizes(uint64_t min, uint64_t max, double factor) {
  XDCHECK_GT(factor, 1.0);
  std::vector<uint64_t> sizes;
  while (min < max) {
    sizes.push_back(min);
    min = util::narrow_cast<uint64_t>(min * factor);
  }
  sizes.push_back(max);
  return sizes;
}
} // namespace

SizeDistribution::SizeDistribution(uint64_t min, uint64_t max, double factor) {
  auto sizes = generateSizes(min, max, factor);
  for (auto size : sizes) {
    dist_.emplace(size, AtomicCounter{});
  }
}

SizeDistribution::SizeDistribution(std::map<int64_t, int64_t> snapshot) {
  for (const auto& kv : snapshot) {
    dist_.emplace(static_cast<uint64_t>(kv.first),
                  AtomicCounter{static_cast<uint64_t>(kv.second)});
  }
}

void SizeDistribution::addSize(uint64_t size) {
  // It's possible user warm-rolled cache from a version without
  // SizeDistribution support. We will remove this once we bring an ice-roll.
  if (dist_.empty()) {
    return;
  }

  auto res =
      std::lower_bound(dist_.begin(), dist_.end(), size, [](auto itr1, auto s) {
        return itr1.first < s;
      });
  XDCHECK_NE(res, dist_.end());
  res->second.add(size);
}

void SizeDistribution::removeSize(uint64_t size) {
  // It's possible user warm-rolled cache from a version without
  // SizeDistribution support. We will remove this once we bring an ice-roll.
  if (dist_.empty()) {
    return;
  }

  auto res =
      std::lower_bound(dist_.begin(), dist_.end(), size, [](auto itr1, auto s) {
        return itr1.first < s;
      });
  XDCHECK_NE(res, dist_.end());
  res->second.sub(size);
}

std::map<int64_t, int64_t> SizeDistribution::getSnapshot() const {
  std::map<int64_t, int64_t> snapshot;
  for (auto& kv : dist_) {
    snapshot.emplace(static_cast<int64_t>(kv.first),
                     static_cast<int64_t>(kv.second.get()));
  }
  return snapshot;
}

void SizeDistribution::reset() {
  for (auto& d : dist_) {
    d.second.set(0);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

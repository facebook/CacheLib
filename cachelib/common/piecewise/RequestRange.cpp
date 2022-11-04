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

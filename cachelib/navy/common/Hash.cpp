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

#include "cachelib/navy/common/Hash.h"

#include <folly/hash/Checksum.h>
#include <folly/hash/Hash.h>

namespace facebook {
namespace cachelib {
namespace navy {
uint64_t hashBuffer(BufferView key, uint64_t seed) {
  return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), seed);
}

uint32_t checksum(BufferView data, uint32_t startingChecksum) {
  return folly::crc32(data.data(), data.size(), startingChecksum);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

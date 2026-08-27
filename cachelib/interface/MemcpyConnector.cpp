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

#include "cachelib/interface/MemcpyConnector.h"

#include <folly/logging/xlog.h>

#include <cstring>
#include <utility>

namespace facebook::cachelib::interface {

folly::coro::Task<Result<AllocatedDescriptor>> MemcpyConnector::transfer(
    ReadDescriptor source, AllocatedDescriptor destination) {
  // Accessing a moved-from descriptor would dereference its empty handle, so
  // check before touching either one.
  if (!source || !destination) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "moved-from descriptor");
  }
  // Exact, not >=: capacity() is the value length -- getMemorySize(), which
  // both RAM and flash tiers report net of allocation rounding.
  const auto size = source.size();
  if (destination.capacity() != size) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "destination size does not match source");
  }
  if (size == 0) {
    co_return std::move(destination);
  }

  // Unreachable once the descriptors are non-empty: every CacheItem returns
  // real memory for a non-zero size. Debug-only, since a violation would be a
  // broken component rather than a caller error.
  void* to = destination.mutableData();
  XDCHECK_NE(to, nullptr) << "destination descriptor has no memory";
  const void* from = source.data();
  XDCHECK_NE(from, nullptr) << "source descriptor has no memory";
  std::memcpy(to, from, size);
  co_return std::move(destination);
}

} // namespace facebook::cachelib::interface

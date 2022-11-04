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

#include "cachelib/allocator/CacheStats.h"

namespace facebook {
namespace cachelib {

/**
 * A virtual interface for compact cache
 */
class ICompactCache {
 public:
  ICompactCache() {}
  virtual ~ICompactCache() {}

  // return the name of the compact cache.
  virtual std::string getName() const = 0;

  // return the pool id for the compact cache
  virtual PoolId getPoolId() const = 0;

  // get the size of the ccache's allocator (in bytes). Returns 0 if it is
  // disabled.
  virtual size_t getSize() const = 0;

  // get the config size of the compact cache
  virtual size_t getConfiguredSize() const = 0;

  // get the stats about the compact cache
  virtual CCacheStats getStats() const = 0;

  // resize the compact cache according to configured size
  virtual void resize() = 0;
};
} // namespace cachelib
} // namespace facebook

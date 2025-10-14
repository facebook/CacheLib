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

#include <folly/coro/Task.h>

#include "cachelib/interface/CacheItem.h"
#include "cachelib/interface/Handle.h"

namespace facebook::cachelib::interface {

class CacheComponent {
 public:
  virtual ~CacheComponent() = default;

  // ------------------------------ Interface ------------------------------ //

  /**
   * Get the name of the component. Should not change throughout the lifetime of
   * the cache component since it will be used to identify items during dynamic
   * reconfiguration.
   *
   * @return the name of the component
   */
  virtual const std::string& getName() const noexcept = 0;

 private:
  // ------------------------------ Interface ------------------------------ //

  /**
   * Release the item from the cache. Frees the associated allocation and
   * executes any necessary callbacks. Only meant to be called by the component
   * or cache item handles.
   *
   * @param item the item to release
   * @param inserted whether the item was previously inserted into the cache
   * (callbacks are typically only called when it was inserted)
   */
  virtual folly::coro::Task<void> release(CacheItem& item, bool inserted) = 0;

  friend class Handle;
};

} // namespace facebook::cachelib::interface

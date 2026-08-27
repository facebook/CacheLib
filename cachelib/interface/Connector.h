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

#include "cachelib/interface/Descriptor.h"
#include "cachelib/interface/Result.h"

namespace facebook::cachelib::interface {

/**
 * Moves a cache item's bytes from a source component to a destination
 * component.
 */
class Connector {
 public:
  virtual ~Connector() = default;

  /**
   * Transfer a cache item from the source to the destination.
   *
   * @param source describes where to read the item's bytes from
   * @param destination describes where to write the item's bytes to
   * @return the destination on success, so the caller can insert it, or an
   *         error result otherwise
   */
  virtual folly::coro::Task<Result<AllocatedDescriptor>> transfer(
      ReadDescriptor source, AllocatedDescriptor destination) = 0;
};

} // namespace facebook::cachelib::interface

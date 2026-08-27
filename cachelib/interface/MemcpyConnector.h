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

#include "cachelib/interface/Connector.h"

namespace facebook::cachelib::interface {

/**
 * Copies bytes from an in-memory source to an in-memory destination.
 *
 * The destination must be exactly the size of the source. A smaller one would
 * truncate; a larger one would report the bytes past the value as part of it,
 * since an item's size comes from its allocation rather than from the transfer.
 * Both read back corrupt, so both are rejected and the destination is left
 * unwritten.
 */
class MemcpyConnector final : public Connector {
 public:
  folly::coro::Task<Result<AllocatedDescriptor>> transfer(
      ReadDescriptor source, AllocatedDescriptor destination) override;
};

} // namespace facebook::cachelib::interface

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

#include <functional>

#include "cachelib/interface/CacheItem.h"

namespace facebook::cachelib::interface {

/**
 * Receives a borrowed item when a component evicts it for capacity. The item
 * is valid only for the duration of the callback. A callback that retains or
 * asynchronously transfers the item must convert it to an owning reference or
 * copy it before returning. The callback may be invoked concurrently, must
 * return promptly, and must not throw.
 */
using EvictionCallback = std::function<void(const CacheItem&)>;

} // namespace facebook::cachelib::interface

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

#include <string>

// Conatains common symbols shared across different build targets. We provide
// a thin wrapper in ReadOnlySharedCacheView that depends on symbols used in
// allocator

namespace facebook {
namespace cachelib {
namespace detail {
// identifier for the metadata info
extern const std::string kShmInfoName;

// identifier for the main cache segment
extern const std::string kShmCacheName;

// identifier for the main hash table if used
extern const std::string kShmHashTableName;

// identifier for the auxilary hash table for chained items
extern const std::string kShmChainedItemHashTableName;

} // namespace detail
} // namespace cachelib
} // namespace facebook

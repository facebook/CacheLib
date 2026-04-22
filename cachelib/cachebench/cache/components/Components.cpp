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

#include "cachelib/cachebench/cache/components/Components.h"

using namespace facebook::cachelib::interface;

namespace facebook::cachelib::cachebench {

extern std::unique_ptr<CacheComponent> createRAMCacheComponent(
    const CacheConfig& config);
extern std::unique_ptr<CacheComponent> createFlashCacheComponent(
    const CacheConfig& config);

std::unique_ptr<CacheComponent> createCacheComponent(
    const CacheConfig& config) {
  if (config.allocator == "flash" || config.allocator == "consistent_flash") {
    return createFlashCacheComponent(config);
  } else {
    XCHECK(config.allocator == "RAM")
        << "Unexpected allocator " << config.allocator;
    return createRAMCacheComponent(config);
  }
}

} // namespace facebook::cachelib::cachebench

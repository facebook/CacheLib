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

#include "cachelib/cachebench/util/CacheConfig.h"
#include "cachelib/interface/CacheComponent.h"

namespace facebook::cachelib::cachebench {

constexpr uint64_t KB = 1024ULL;
constexpr uint64_t MB = KB * 1024ULL;

std::unique_ptr<interface::CacheComponent> createRAMCacheComponent(
    const CacheConfig& config);

} // namespace facebook::cachelib::cachebench

/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include <cstdint>
#include <memory>
#include <string>

#include "cachelib/allocator/ReadOnlySharedCacheView.h"

namespace facebook {
namespace rust {
namespace cachelib {

std::unique_ptr<facebook::cachelib::ReadOnlySharedCacheView>
ro_cache_view_attach(const std::string& cache_dir);

std::unique_ptr<facebook::cachelib::ReadOnlySharedCacheView>
ro_cache_view_attach_at_address(const std::string& cache_dir, size_t);

uintptr_t ro_cache_view_get_shm_mapping_address(
    const facebook::cachelib::ReadOnlySharedCacheView& cache);

const uint8_t* ro_cache_view_get_item_ptr_from_offset(
    const facebook::cachelib::ReadOnlySharedCacheView& cacheView,
    size_t offset);
} // namespace cachelib
} // namespace rust
} // namespace facebook

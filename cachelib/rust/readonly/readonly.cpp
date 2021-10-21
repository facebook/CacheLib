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

#include "cachelib/rust/readonly/readonly.h"

namespace facebook {
namespace rust {
namespace cachelib {

std::unique_ptr<facebook::cachelib::ReadOnlySharedCacheView>
ro_cache_view_attach(const std::string& cache_dir) {
  return std::make_unique<facebook::cachelib::ReadOnlySharedCacheView>(
      cache_dir, false);
}

std::unique_ptr<facebook::cachelib::ReadOnlySharedCacheView>
ro_cache_view_attach_at_address(const std::string& cache_dir, size_t addr) {
  return std::make_unique<facebook::cachelib::ReadOnlySharedCacheView>(
      cache_dir, false, (void*)addr);
}

uintptr_t ro_cache_view_get_shm_mapping_address(
    const facebook::cachelib::ReadOnlySharedCacheView& cacheView) {
  return cacheView.getShmMappingAddress();
}

const uint8_t* ro_cache_view_get_item_ptr_from_offset(
    const facebook::cachelib::ReadOnlySharedCacheView& cacheView,
    size_t offset) {
  return static_cast<const uint8_t*>(
      const_cast<facebook::cachelib::ReadOnlySharedCacheView&>(cacheView)
          .getItemPtrFromOffset(offset));
}

} // namespace cachelib
} // namespace rust
} // namespace facebook

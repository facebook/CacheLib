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

// (c) Facebook, Inc. and its affiliates. Confidential and proprietary.

#pragma once

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::cachelib {

// Wrapper class for external users to access to the private findImpl
// API before we finish the R/W handle migration.
// The APIs are meant to be temporary. We will deprecate these after finishing
// the migration.
template <typename Allocator>
class CacheAllocatorFindApiWrapper {
 public:
  static typename Allocator::ItemHandle findImpl(Allocator& alloc,
                                                 typename Allocator::Key key,
                                                 AccessMode mode) {
    return alloc.findImpl(key, mode);
  }
};

} // namespace facebook::cachelib

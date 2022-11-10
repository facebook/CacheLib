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
#include <folly/dynamic.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/AbstractCache.h"
namespace facebook {
namespace cachelib {
// return a navy cache which is created by CacheProto whose data is from
// NavyConfig.
std::unique_ptr<facebook::cachelib::navy::AbstractCache> createNavyCache(
    const navy::NavyConfig& config,
    facebook::cachelib::navy::ExpiredCheck checkExpired,
    facebook::cachelib::navy::DestructorCallback destructorCb,
    bool truncate,
    std::shared_ptr<navy::DeviceEncryptor> encryptor,
    bool itemDestructorEnabled);

// create a flash device for Navy engines to use
// made public for testing purposes
std::unique_ptr<cachelib::navy::Device> createDevice(
    const navy::NavyConfig& config,
    std::shared_ptr<navy::DeviceEncryptor> encryptor);
} // namespace cachelib
} // namespace facebook

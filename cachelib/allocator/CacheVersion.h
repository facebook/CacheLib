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

#include <cstdint>
#include <string>

namespace facebook {
namespace cachelib {
// If you're bumping kCacheRamFormatVersion or kCacheNvmFormatVersion,
// you *MUST* bump this as well.
//
// If the change is not an incompatible one, but you want to keep track of it,
// then you only need to bump this version.
// I.e. you're rolling out a new feature that is cache compatible with previous
// Cachelib instances.
constexpr uint64_t kCachelibVersion = 17;

// Updating this version will cause RAM cache to be dropped for all
// cachelib users!!! Proceed with care!! You must coordinate with
// cachelib users either directly or via Cache Library group.
//
// If you're bumping this version, you *MUST* bump kCachelibVersion
// as well.
constexpr uint64_t kCacheRamFormatVersion = 3;

// Updating this version will cause NVM cache to be dropped for all
// cachelib users!!! Proceed with care!! You must coordinate with
// cachelib users either directly or via Cache Library group.
//
// If you're bumping this version, you *MUST* bump kCachelibVersion
// as well.
constexpr uint64_t kCacheNvmFormatVersion = 2;

// @return a string as version.
// cachelib: X, ram: Y, nvm: Z
inline const std::string& getCacheVersionString() {
  static std::string kVersionStr =
      "{ \"cachelib\" : " + std::to_string(kCachelibVersion) +
      ", \"ram\" : " + std::to_string(kCacheRamFormatVersion) +
      ", \"nvm\" : " + std::to_string(kCacheNvmFormatVersion) + "}";
  return kVersionStr;
}
} // namespace cachelib
} // namespace facebook

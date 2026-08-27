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

#include "cachelib/interface/CacheItem.h"

namespace facebook::cachelib::interface {

/**
 * A detached copy of a cache item's key, value, and metadata, for where the
 * source cannot be kept alive by a handle.
 */
class DetachedItem {
 public:
  explicit DetachedItem(const CacheItem& source)
      : key_(source.getKey().str()),
        creationTime_(source.getCreationTime()),
        expiryTime_(source.getExpiryTime()) {
    const auto size = source.getMemorySize();
    if (size != 0) {
      value_.assign(static_cast<const char*>(source.getMemory()), size);
    }
  }

  uint32_t getCreationTime() const noexcept { return creationTime_; }

  uint32_t getExpiryTime() const noexcept { return expiryTime_; }

  Key getKey() const noexcept { return key_; }

  void* getMemory() const noexcept { return const_cast<char*>(value_.data()); }

  uint32_t getMemorySize() const noexcept {
    return static_cast<uint32_t>(value_.size());
  }

 private:
  std::string key_;
  std::string value_;
  uint32_t creationTime_;
  uint32_t expiryTime_;
};

} // namespace facebook::cachelib::interface

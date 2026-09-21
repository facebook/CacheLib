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
#include <exception>
#include <string>
#include <utility>

#include "cachelib/interface/DetachedItem.h"

namespace facebook::cachelib::interface::test {

class FakeCacheItem final : public CacheItem {
 public:
  FakeCacheItem(std::string key,
                std::string value,
                uint32_t creationTime,
                uint32_t expiryTime)
      : key_(std::move(key)),
        value_(std::move(value)),
        creationTime_(creationTime),
        expiryTime_(expiryTime) {}

  uint32_t getCreationTime() const noexcept override { return creationTime_; }
  uint32_t getExpiryTime() const noexcept override { return expiryTime_; }
  UnitResult incrementRefCount(CacheComponent&) noexcept override {
    return folly::unit;
  }
  bool decrementRefCount(CacheComponent&) noexcept override { return false; }
  Key getKey() const noexcept override { return key_; }
  void* getMemory() const noexcept override {
    return const_cast<char*>(value_.data());
  }
  uint32_t getMemorySize() const noexcept override {
    return static_cast<uint32_t>(value_.size());
  }
  uint32_t getTotalSize() const noexcept override {
    return static_cast<uint32_t>(sizeof(*this) + key_.size() + value_.size());
  }

 private:
  void move(void*) noexcept override { std::terminate(); }

  std::string key_;
  std::string value_;
  uint32_t creationTime_;
  uint32_t expiryTime_;
};

inline DetachedItem makeDetachedItem(const std::string& key,
                                     const std::string& value,
                                     uint32_t creationTime = 0,
                                     uint32_t expiryTime = 0) {
  const FakeCacheItem item(key, value, creationTime, expiryTime);
  return DetachedItem(item);
}

} // namespace facebook::cachelib::interface::test

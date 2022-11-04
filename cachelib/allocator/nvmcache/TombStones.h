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
#include <folly/container/F14Map.h>
#include <folly/lang/Align.h>
#include <glog/logging.h>

#include <mutex>
#include <utility>

#include "folly/Range.h"

namespace facebook {
namespace cachelib {

// Utility that helps us track in flight deletes. We maintain a count per key
// and check for presence against the count to resolve multiple concurrent
// deletes for the same key in flight.
class alignas(folly::hardware_destructive_interference_size) TombStones {
 public:
  class Guard;

  // adds an instance of  key
  // @param key  key for the record
  // @return a valid Guard representing the tombstone
  Guard add(folly::StringPiece key) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = keys_.find(key);

    if (it == keys_.end()) {
      it = keys_.insert(std::make_pair(key.toString(), 0)).first;
    }

    ++it->second;
    return Guard(it->first, *this);
  }

  // checks if there is a key present and returns true if so.
  bool isPresent(folly::StringPiece key) {
    std::lock_guard<std::mutex> l(mutex_);
    return keys_.count(key) != 0;
  }

  // Guard that wraps around the tombstone record. Removes the key from the
  // tombstone records upon destruction. A valid guard can be only created by
  // adding to the tombstone record.
  class Guard {
   public:
    Guard() {}
    ~Guard() {
      if (tombstones_) {
        tombstones_->remove(key_);
        tombstones_ = nullptr;
      }
    }

    // disable copying
    Guard(const Guard&) = delete;
    Guard& operator=(const Guard&&) = delete;

    // allow moving
    Guard(Guard&& other) noexcept
        : key_{std::move(other.key_)}, tombstones_(other.tombstones_) {
      other.tombstones_ = nullptr;
    }
    Guard& operator=(Guard&& other) noexcept {
      if (this != &other) {
        this->~Guard();
        new (this) Guard(std::move(other));
      }
      return *this;
    }

    folly::StringPiece key() const noexcept { return key_; }

    explicit operator bool() const noexcept { return tombstones_ != nullptr; }

   private:
    // only tombstone can create a guard.
    friend TombStones;
    Guard(folly::StringPiece key, TombStones& t) noexcept
        : key_(key), tombstones_(&t) {}

    // key for the tombstone
    folly::StringPiece key_;

    // tombstone record
    TombStones* tombstones_{nullptr};
  };

 private:
  // removes an instance of key. if the count drops to 0, we remove the key
  void remove(folly::StringPiece key) {
    std::lock_guard<std::mutex> l(mutex_);
    auto it = keys_.find(key);
    if (it == keys_.end() || it->second == 0) {
      // this is not supposed to happen if guards are destroyed appropriately
      throw std::runtime_error(fmt::format(
          "Invalid state. Key: {}. State: {}", key,
          it == keys_.end() ? "does not exist" : "exists, but count is 0"));
    }

    if (--(it->second) == 0) {
      keys_.erase(it);
    }
  }

  // mutex protecting the map below
  std::mutex mutex_;
  folly::F14NodeMap<std::string, uint64_t> keys_;
};

} // namespace cachelib
} // namespace facebook

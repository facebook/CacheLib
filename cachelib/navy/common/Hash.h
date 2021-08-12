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

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Default hash function
uint64_t hashBuffer(BufferView key, uint64_t seed = 0);

// Default checksumming function
uint32_t checksum(BufferView data, uint32_t startingChecksum = 0);

// Pairs up key and hash together, eliminating possibility to modify one of
// them independently.
class HashedKey {
 public:
  static HashedKey precomputed(BufferView key, uint64_t keyHash) {
    return HashedKey{key, keyHash};
  }

  // @param key   key BufferView
  explicit HashedKey(BufferView key) : HashedKey{key, hashBuffer(key)} {}

  BufferView key() const { return key_; }

  uint64_t keyHash() const { return keyHash_; }

  bool operator==(HashedKey other) const {
    return keyHash_ == other.keyHash_ && key_ == other.key_;
  }

  bool operator!=(HashedKey other) const { return !(*this == other); }

 private:
  HashedKey(BufferView key, uint64_t keyHash) : key_{key}, keyHash_{keyHash} {}

  BufferView key_;
  uint64_t keyHash_{};
};

// Convenience utils to convert a piece of buffer to a hashed key
inline HashedKey makeHK(BufferView key) { return HashedKey{key}; }
inline HashedKey makeHK(const Buffer& key) { return HashedKey{key.view()}; }
inline HashedKey makeHK(const char* cstr) { return HashedKey{makeView(cstr)}; }
} // namespace navy
} // namespace cachelib
} // namespace facebook

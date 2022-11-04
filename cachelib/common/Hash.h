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

#include <folly/hash/Hash.h>

#include "cachelib/common/BytesEqual.h"
#include "cachelib/common/MurmurHash.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {

// Hasher object for the hash table.
struct Hash {
  virtual ~Hash() = default;
  virtual uint32_t operator()(const void* buf, size_t n) const noexcept = 0;
  virtual int getMagicId() const noexcept = 0;
};
using Hasher = std::shared_ptr<Hash>;

struct FNVHash final : public Hash {
  uint32_t operator()(const void* buf, size_t n) const noexcept override {
    return folly::hash::fnv32_buf(buf, n, folly::hash::fnv32_hash_start);
  }
  int getMagicId() const noexcept override { return 1; }
};

struct MurmurHash2 final : public Hash {
  uint32_t operator()(const void* buf, size_t n) const noexcept override {
    return facebook::cachelib::murmurHash2(buf, static_cast<int>(n),
                                           kMurmur2Seed);
  }
  int getMagicId() const noexcept override { return 2; }

  constexpr static uint64_t kMurmur2Seed = 4193360111ul;
};

// a stateless consistent hash function
//
// This function accepts a "buf" of length "len" and a value "range" that
// establishes the range of output to be [0 : (range-1)]. The result will be
// uniformly distributed within that range based on the key, and has the
// property that changes in "range" will produce the minimum amount of re-
// distribution of keys.
//
// For example, if "range" is increased from 11 to 12, 1/12th of keys for each
// output value [0 : 10] will be reassigned the value of 11 while the
// remaining 11/12th of keys will produce the same value as before.
//
// On average a call to this function will take less than 400ns for "range" up
// to 131071 (average key length 13 bytes), but there is a small chance that it
// will take several times this, up to 4us in very rare cases.  It uses
// MurmurHash64A() internally, a hash function with both fast performance
// and excellent statistical properties. This endows furcHash() with good
// performance even for longer keys.
uint32_t furcHash(const void* buf, size_t len, uint32_t range);

// combines two 64 bit hash into one
inline uint64_t combineHashes(uint64_t h1, uint64_t h2) {
  return folly::hash::hash_128_to_64(h1, h2);
}

inline uint64_t hashInt(uint64_t key) { return folly::hash::twang_mix64(key); }

// Pairs up key and hash together, reducing the cost of computing hash multiple
// times, and eliminating possibility to modify one of them independently.
class HashedKey {
 public:
  static HashedKey precomputed(folly::StringPiece key, uint64_t keyHash) {
    return HashedKey{key, keyHash};
  }

  explicit HashedKey(folly::StringPiece key)
      : HashedKey{key, hashBuffer(key)} {}

  HashedKey(const char* key, size_t size)
      : HashedKey{folly::StringPiece{key, size}} {}

  folly::StringPiece key() const { return key_; }

  uint64_t keyHash() const { return keyHash_; }

  bool operator==(HashedKey other) const {
    return keyHash_ == other.keyHash_ && key_.size() == other.key().size() &&
           bytesEqual(key_.data(), other.key().data(), key_.size());
  }

  bool operator!=(HashedKey other) const { return !(*this == other); }

 private:
  HashedKey(folly::StringPiece key, uint64_t keyHash)
      : key_{key}, keyHash_{keyHash} {}

  // copy from navy/common/Hash.h to keep consistent hash behavior for keys
  // TODO: may use MurmurHash2 later
  static uint64_t hashBuffer(folly::StringPiece key, uint64_t seed = 0) {
    return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), seed);
  }

  folly::StringPiece key_;
  uint64_t keyHash_{};
};

} // namespace cachelib
} // namespace facebook

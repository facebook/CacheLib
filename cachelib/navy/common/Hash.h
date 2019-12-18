#pragma once

#include "cachelib/navy/common/Buffer.h"

#include <cstdint>

namespace facebook {
namespace cachelib {
namespace navy {
// Default hash function
uint64_t hashBuffer(BufferView key, uint64_t seed = 0);
uint64_t hashInt(uint64_t key);
uint64_t combineHashes(uint64_t h1, uint64_t h2);

// Default checksumming function
uint32_t checksum(BufferView data, uint32_t startingChecksum = 0);

// Pairs up key and hash together, eliminating possibility to modify one of
// them independently.
class HashedKey {
 public:
  static HashedKey precomputed(BufferView key, uint64_t keyHash) {
    return HashedKey{key, keyHash};
  }

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

inline HashedKey makeHK(BufferView key) { return HashedKey{key}; }

inline HashedKey makeHK(const Buffer& key) { return HashedKey{key.view()}; }

inline HashedKey makeHK(const char* cstr) { return HashedKey{makeView(cstr)}; }
} // namespace navy
} // namespace cachelib
} // namespace facebook

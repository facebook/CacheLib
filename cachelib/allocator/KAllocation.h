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

#include <limits>
#include <stdexcept>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/String.h>

#include "cachelib/common/BytesEqual.h"
#include "cachelib/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {

/**
 * Represents an allocation with a key. Provides a byte array interface that
 * holds the key and a contiguous space of memory.
 */
class CACHELIB_PACKED_ATTR KAllocation {
 public:
  using KeyLenT = uint8_t;
  // Maximum size of the key.
  static constexpr KeyLenT kKeyMaxLen = std::numeric_limits<KeyLenT>::max();
  // Maximum number of bits of the value (payload minus the key)
  static constexpr uint32_t kMaxValSizeBits =
      NumBits<uint32_t>::value - NumBits<KeyLenT>::value;
  // Maximum size of the value (payload minus the key)
  static constexpr uint32_t kMaxValSize =
      (static_cast<uint32_t>(1) << kMaxValSizeBits) - 1;

  // type of the key for allocations. It is a folly::StringPiece aka
  // Range<const char*> with a custom comparison operator that should do
  // better than folly's compare.
  class Key : public folly::StringPiece {
   public:
    using folly::StringPiece::StringPiece;

    /* implicit */
    Key(folly::StringPiece rhs) : folly::StringPiece(rhs) {}

    bool operator==(Key other) const {
      return size() == other.size() && bytesEqual(data(), other.data(), size());
    }

    bool operator!=(Key other) const { return !(*this == other); }
  };

  // constructs the KAllocation instance by initializing the key and the
  // appropriate fields. The key is copied into the allocation.
  //
  // total size of variable allocation in KAllocation:
  //  valSize + keySize
  //
  // @param key       the key for the allocation
  // @param valSize   size of the value
  //
  // @throw std::invalid_argument if the key/size is invalid.
  KAllocation(const Key key, uint32_t valSize)
      : size_((static_cast<uint32_t>(key.size()) << kMaxValSizeBits) |
              valSize) {
    if (valSize > kMaxValSize) {
      throw std::invalid_argument(folly::sformat(
          "value size exceeded maximum allowed. total size: {}", valSize));
    }

    throwIfKeyInvalid(key);

    // Copy the key into the allocation
    memcpy(&data_[0], key.start(), getKeySize());
  }

  KAllocation(const KAllocation&) = delete;
  KAllocation& operator=(const KAllocation&) = delete;

  // returns the key corresponding to the allocation.
  const Key getKey() const noexcept {
    return Key{reinterpret_cast<char*>(&data_[0]), getKeySize()};
  }

  // updates the current key with the new one. The key size must match.
  void changeKey(Key key) {
    if (key.size() != getKeySize()) {
      throw std::invalid_argument("Key size mismatch");
    }
    std::memcpy(&data_[0], key.start(), getKeySize());
  }

  // return a void* to the usable memory block. There are no alignment
  // guarantees.
  // TODO add support for alignment
  void* getMemory() const noexcept { return &data_[getKeySize()]; }

  // get the size of the value.
  uint32_t getSize() const noexcept { return size_ & kMaxValSize; }

  // Check if the key is valid.  The length of the key needs to be in (0,
  // kKeyMaxLen) to be valid
  static bool isKeyValid(folly::StringPiece key) {
    // StringPiece empty() does not realy check for start being nullptr
    return (key.size() <= kKeyMaxLen) && (!key.empty()) && (key.start());
  }

  // Throw readable exception if the key is invalid.
  static void throwIfKeyInvalid(folly::StringPiece key) {
    if (!isKeyValid(key)) {
      // We need to construct the key for the error message manually here for
      // two reasons
      //
      // 1) The StringPiece can start with a nullptr, and have a non-0 length,
      //    this in turn means that std::string's constructor will throw a
      //    std::logic_error.  So we construct the string manually
      // 2) The StringPiece might not be null terminated.  So we construct the
      //    std::string manually with a pointer and size.  Which mandates good
      //    internal representation
      auto badKey =
          (key.start()) ? std::string(key.start(), key.size()) : std::string{};
      throw std::invalid_argument{
          folly::sformat("Invalid cache key : {}", folly::humanify(badKey))};
    }
  }

 private:
  // Top 8 bits are for key size (up to 255 bytes)
  // Bottom 24 bits are for value size (up to 16777215 bytes)
  const uint32_t size_;

  // beginning of the byte array. First keylen bytes correspond to the key and
  // the next size - keylen_ bytes are usable.
  mutable unsigned char data_[0];

  uint32_t getKeySize() const noexcept {
    return static_cast<uint32_t>(size_ >> kMaxValSizeBits);
  }
};
} // namespace cachelib
} // namespace facebook

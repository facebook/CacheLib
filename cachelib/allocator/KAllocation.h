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
  // KAllocation supports keys > 255 bytes, but optimizes for small keys (<= 255
  // bytes). If the key is > 255 bytes, KAllocation sets a marker value and
  // stores an extra 4 size bytes before the key itself.
  using SmallKeyLenT = uint8_t;
  // Maximum size of small keys.
  static constexpr SmallKeyLenT kKeyMaxLenSmall =
      std::numeric_limits<SmallKeyLenT>::max();
  static constexpr uint32_t kMaxKeySizeBitsSmall = NumBits<SmallKeyLenT>::value;

  // Maximum number of bits of the value (payload minus the key)
  static constexpr uint32_t kMaxValSizeBits =
      NumBits<uint32_t>::value - kMaxKeySizeBitsSmall;
  // Maximum size of the value (payload minus the key)
  static constexpr uint32_t kMaxValSize =
      (static_cast<uint32_t>(1) << kMaxValSizeBits) - 1;

  using LargeKeyLenT = uint32_t;
  // maximum size of large keys. limit size to max value size since we can
  // overflow the total allocation size if we set to uint32_t max.
  static constexpr LargeKeyLenT kKeyMaxLen = kMaxValSize;

  // returns extra bytes required (if any) to store the key size for large keys
  // @param keySize   size of the key
  // @return          extra bytes required to store the key size if key is > 255
  //                  bytes, 0 otherwise
  static constexpr uint64_t extraBytesForLargeKeys(uint64_t keySize) {
    return keySize <= kKeyMaxLenSmall ? 0 : sizeof(LargeKeyLenT);
  }

  // type of the key for allocations. It is a folly::StringPiece aka
  // Range<const char*> with a custom comparison operator that should do
  // better than folly's compare.
  class Key : public folly::StringPiece {
   public:
    using folly::StringPiece::StringPiece;

    explicit Key(const std::string& key) : folly::StringPiece(key) {}

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
  KAllocation(const Key key, uint32_t valSize) {
    if (valSize > kMaxValSize) {
      throw std::invalid_argument(folly::sformat(
          "value size exceeded maximum allowed. total size: {}", valSize));
    }

    throwIfKeyInvalid(key);

    size_.valSize_ = valSize;
    if (key.size() <= kKeyMaxLenSmall) {
      size_.keySize_ = static_cast<uint32_t>(key.size());
    } else {
      size_.keySize_ = 0; // signal that the key is large
      reinterpret_cast<LargeKey*>(data_)->largeKeySize_ =
          static_cast<uint32_t>(key.size());
    }

    // Copy the key into the allocation
    memcpy(getData(), key.start(), key.size());
  }

  // Can't use default versions of copy/move constructors since there may be an
  // implicit large key size, explicitly delete to prevent misuse
  KAllocation(const KAllocation&) = delete;
  KAllocation& operator=(const KAllocation&) = delete;
  KAllocation(KAllocation&&) = delete;
  KAllocation& operator=(KAllocation&&) = delete;
  ~KAllocation() = default;

  // returns the key corresponding to the allocation.
  const Key getKey() const noexcept {
    // Reaper will call this function on unallocated data, so ensure key size is
    // within the limit
    auto keySize = std::min(kKeyMaxLen, getKeySize());
    return Key{reinterpret_cast<char*>(getData()), keySize};
  }

  // same as getKey() but ensures we don't create a key that could extend
  // outside the given allocation size (safe to call on unallocated data)
  const Key getKeySized(uint32_t allocSize) const noexcept {
    uint32_t headerSize = isSmallKey() ? sizeof(PackedSize)
                                       : sizeof(PackedSize) + sizeof(LargeKey);
    auto keySize = std::min(allocSize - headerSize, getKeySize());
    return Key{reinterpret_cast<char*>(getData()), keySize};
  }

  // updates the current key with the new one. The key size must match.
  void changeKey(Key key) {
    if (key.size() != getKeySize()) {
      throw std::invalid_argument("Key size mismatch");
    }
    memcpy(getData(), key.start(), key.size());
  }

  // return a void* to the usable memory block. There are no alignment
  // guarantees.
  // TODO add support for alignment
  void* getMemory() const noexcept { return &getData()[getKeySize()]; }

  // get the size of the value.
  uint32_t getSize() const noexcept { return size_.valSize_; }

 private:
  // Top 8 bits are for key size (up to 255 bytes)
  // Bottom 24 bits are for value size (up to 16777215 bytes)
  struct PackedSize {
    uint32_t valSize_ : kMaxValSizeBits;
    uint32_t keySize_ : kMaxKeySizeBitsSmall;
  };
  static_assert(sizeof(PackedSize) == sizeof(uint32_t));
  PackedSize size_{};

  // beginning of the byte array. First keylen bytes correspond to the key and
  // the next size - keylen_ bytes are usable.
  // NOTE: if size_.keySize_ == 0, there's an implicit 4 size bytes before the
  // key. Use the helpers below for easier access.
  mutable unsigned char data_[0];

  struct LargeKey {
    uint32_t largeKeySize_;
    unsigned char data_[0];
  };

  bool isSmallKey() const noexcept { return size_.keySize_ != 0; }

  unsigned char* getData() const noexcept {
    return isSmallKey() ? data_ : reinterpret_cast<LargeKey*>(data_)->data_;
  }

  uint32_t getKeySize() const noexcept {
    return isSmallKey() ? size_.keySize_
                        : reinterpret_cast<LargeKey*>(data_)->largeKeySize_;
  }

  // Check if the key is valid.  The length of the key needs to be in (0,
  // kKeyMaxLen) to be valid
  static bool isKeyValid(folly::StringPiece key) {
    // StringPiece empty() does not really check for start being nullptr
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

  // Same as above but for small keys.
  static bool isSmallKeyValid(folly::StringPiece key) {
    return (key.size() <= kKeyMaxLenSmall) && (!key.empty()) && (key.start());
  }

  static void throwIfSmallKeyInvalid(folly::StringPiece key) {
    if (!isSmallKeyValid(key)) {
      auto badKey =
          (key.start()) ? std::string(key.start(), key.size()) : std::string{};
      throw std::invalid_argument{
          folly::sformat("Invalid cache key : {}", folly::humanify(badKey))};
    }
  }

  template <typename T>
  friend class CacheAllocator;
};
} // namespace cachelib
} // namespace facebook

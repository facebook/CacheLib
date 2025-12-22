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

#include <folly/Portability.h>

#include <cassert>

#include "cachelib/common/Serialization.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

// Base interface class for NVM index: map from key to value.
// Under the hood, it stores key hash to value map
class Index {
 public:
  // Specify 1 second window size for quantile estimator with any Index.
  // TODO: this will be deprecated when we deprecated totalHits tracking.
  static constexpr std::chrono::seconds kQuantileWindowSize{1};

  virtual ~Index() = default;

  // ItemRecord is the structure that will be used for LookupResult used by
  // Index's APIs.
  // (Though we will use a different format for actual stored payload in memory
  // per different types of Index implementation, we keep this strucutre as it
  // is for now since it's being used in interface's return type)
  // TODO: totalHits will be deprecated soon.
  struct FOLLY_PACK_ATTR ItemRecord {
    // encoded address
    uint32_t address{0};
    // a hint for the item size. It is the responsibility of the caller
    // to translate this hint back into a usable size.
    uint16_t sizeHint{0};
    // either total hits during this item's entire lifetime in cache or
    // binary representation of item hits history (MSB is 1 if item had hits in
    // the last write cycle, LSB represents 8 cycles ago), depending on the
    // BlockCacheIndexConfig
    union {
      uint8_t totalHits;
      uint8_t itemHistory;
    } extra{0};

    // hits during the current window for this item (e.g. before re-admission)
    uint8_t currentHits{0};
    ItemRecord(uint32_t _address = 0,
               uint16_t _sizeHint = 0,
               uint8_t _extra = 0,
               uint8_t _currentHits = 0)
        : address(_address),
          sizeHint(_sizeHint),
          extra{.totalHits = _extra},
          currentHits(_currentHits) {}
  };
  static_assert(8 == sizeof(ItemRecord), "ItemRecord size is 8 bytes");

  struct LookupResult {
    friend class Index;

    LookupResult() = default;
    explicit LookupResult(bool found, ItemRecord record)
        : record_(record), found_(found) {}

    bool found() const { return found_; }

    ItemRecord record() const {
      XDCHECK(found_);
      return record_;
    }

    uint32_t address() const {
      XDCHECK(found_);
      return record_.address;
    }

    uint16_t sizeHint() const {
      XDCHECK(found_);
      return record_.sizeHint;
    }

    uint8_t currentHits() const {
      XDCHECK(found_);
      return record_.currentHits;
    }

    uint8_t totalHits() const {
      XDCHECK(found_);
      return record_.extra.totalHits;
    }

    uint8_t itemHistory() const {
      XDCHECK(found_);
      return record_.extra.itemHistory;
    }

   private:
    ItemRecord record_;
    bool found_{false};
  };

  struct MemFootprintRange {
    size_t maxUsedBytes{0};
    size_t minUsedBytes{0};
  };

  // Internally, FixedSizeIndex will maintain each entry as PackedItemRecord
  // which is reduced size version of Index::ItemRecord, and there is missing
  // precision or info due to the smaller size, but those missing details are
  // not critical ones.
  struct FOLLY_PACK_ATTR PackedItemRecord {
    // encoded address
    uint32_t address{kInvalidAddress};
    // info about size and current hits.
    struct {
      uint8_t curHits : 2;
      uint8_t sizeExp : 6;
    } info{};

    static constexpr double kSizeExpBase = 1.196;
    // Rather than using extra bit for indicating combined entry in index, this
    // specific size value will be used.
    static constexpr uint8_t kCombinedEntrySizeExp = 0x3f;

    // Instead of using 1-bit for a flag per item to say if it's a valid entry
    // or not, we will use the pre-defined invalid address value to decide the
    // validity. With the current block cache implementation, address 0 won't be
    // used for index address, so we are using it as the invalid address value
    // here.
    //
    // (Current BC implementation/design always stores the end of the slot
    // address (for the entry), so it will be always the address of the end of
    // entry descriptor. See BlockCache.h for more details)
    static constexpr uint32_t kInvalidAddress{0};

    PackedItemRecord() {}

    PackedItemRecord(uint32_t _address,
                     uint16_t _sizeHint,
                     uint8_t _currentHits)
        : address(_address) {
      info.curHits = truncateCurHits(_currentHits);
      info.sizeExp = sizeHintToExp(_sizeHint);
      XDCHECK(isValidAddress(_address));
    }

    bool operator==(const PackedItemRecord& other) const noexcept {
      return (address == other.address) &&
             (info.curHits == other.info.curHits) &&
             (info.sizeExp == other.info.sizeExp);
    }

    static uint8_t sizeHintToExp(uint16_t sizeHint) {
      // Input value (sizeHint) is the unit of kMinAllocAlignSize
      // (i.e. sizeHint = 1 means 512Bytes currently).
      // We want to represent this 16bit value by exponent value with 6bits
      // while 0x3F (all '1's, or 63 in decimal) will be reserved to indicate
      // it's a combined entry bucket.
      // So (a ^ 0) = 0, (a ^ 62) >= max value (65535), then we will use a
      // == 1.196. So we can represent sizeHint by the exponent of base 1.196.
      // (All these exponent usage will be improved as described as TODO below,
      // but for now, for simplicity, this exponent convention is used here)
      // TODO1: Will remove using exponents and multiplications and improve here
      // TODO2: Need to revisit and evaluate to see if we need the same
      // precision level for the larger sizes

      XDCHECK(sizeHint > 0) << sizeHint;
      constexpr double m = kSizeExpBase;
      double x = 1;
      int xp = 0;
      while (x < sizeHint) {
        x *= m;
        ++xp;
      }
      XDCHECK(xp < kCombinedEntrySizeExp) << sizeHint << " " << xp;
      return static_cast<uint8_t>(xp);
    }

    static uint16_t sizeExpToHint(uint8_t sizeExp) {
      // TODO: Will remove using exponents and multiplications and improve here
      constexpr double m = kSizeExpBase;
      double sizeHint = 1;
      for (int j = 0; j < sizeExp; ++j) {
        sizeHint *= m;
      }
      return static_cast<uint16_t>(sizeHint);
    }

    static uint8_t truncateCurHits(uint8_t curHits) {
      return (curHits > 3) ? 3 : curHits;
    }

    static bool isValidAddress(uint32_t address) {
      return address != kInvalidAddress;
    }

    bool isValid() const { return isValidAddress(address); }
    uint16_t getSizeHint() const { return sizeExpToHint(info.sizeExp); }

    int bumpCurHits() {
      if (info.curHits < 3) {
        info.curHits++;
      }
      return info.curHits;
    }
  };
  static_assert(5 == sizeof(PackedItemRecord),
                "PackedItemRecord size is 5 bytes");

  // Writes index to a Thrift object or shm.
  // When the index uses shm for its persistence, RecordWriter is not needed.
  virtual void persist(
      std::optional<std::reference_wrapper<RecordWriter>> rw) const = 0;

  // Resets index then inserts entries read from @deserializer or shm.
  // When the index uses shm for its persistence, RecordReader is not needed.
  virtual void recover(
      std::optional<std::reference_wrapper<RecordReader>> rr) = 0;

  // Gets value and update tracking counters
  virtual LookupResult lookup(uint64_t key) = 0;

  // Gets value without updating tracking counters
  virtual LookupResult peek(uint64_t key) const = 0;

  // Inserts or overwrites existing key if exists with new address and size, and
  // it also will reset hits counting. If the entry was successfully
  // overwritten, LookupResult.found() returns true and LookupResult.record()
  // returns the old record.
  virtual LookupResult insert(uint64_t key,
                              uint32_t address,
                              uint16_t sizeHint) = 0;
  // Same as above but fails if the key already exists
  virtual LookupResult insertIfNotExists(uint64_t key,
                                         uint32_t address,
                                         uint16_t sizeHint) = 0;

  // Replaces old address with new address if there exists the key with the
  // identical old address. Current hits will be reset after successful replace.
  // All other fields in the record is retained.
  //
  // @return true if replaced.
  virtual bool replaceIfMatch(uint64_t key,
                              uint32_t newAddress,
                              uint32_t oldAddress) = 0;

  // If the entry was successfully removed, LookupResult.found() returns true
  // and LookupResult.record() returns the record that was just found.
  // If the entry wasn't found, then LookupResult.found() returns false.
  virtual LookupResult remove(uint64_t key) = 0;

  // Removes only if both key and address match.
  //
  // @return true if removed successfully, false otherwise.
  virtual bool removeIfMatch(uint64_t key, uint32_t address) = 0;

  // Resets all the buckets to the initial state.
  virtual void reset() = 0;

  // Walks buckets and computes total index entry count
  virtual size_t computeSize() const = 0;

  // Walks buckets and computes max/min memory footprint range that index will
  // currently use for the entries it currently has. (Since there could be cases
  // that it's difficult to get exact number with the memory footprint (ex.
  // sparse_map) this function should return max/min range of memory footprint
  virtual MemFootprintRange computeMemFootprintRange() const = 0;

  // Exports index stats via CounterVisitor.
  virtual void getCounters(const CounterVisitor& visitor) const = 0;

 protected:
  // Updates hits information of a key.
  virtual void setHitsTestOnly(uint64_t key,
                               uint8_t currentHits,
                               uint8_t totalHits) = 0;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

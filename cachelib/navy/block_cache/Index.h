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
    // total hits during this item's entire lifetime in cache
    uint8_t totalHits{0};
    // hits during the current window for this item (e.g. before re-admission)
    uint8_t currentHits{0};

    ItemRecord(uint32_t _address = 0,
               uint16_t _sizeHint = 0,
               uint8_t _totalHits = 0,
               uint8_t _currentHits = 0)
        : address(_address),
          sizeHint(_sizeHint),
          totalHits(_totalHits),
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
      return record_.totalHits;
    }

   private:
    ItemRecord record_;
    bool found_{false};

    friend class SparseMapIndex;
  };

  struct MemFootprintRange {
    size_t maxUsedBytes{0};
    size_t minUsedBytes{0};
  };

  // Writes index to a Thrift object
  virtual void persist(RecordWriter& rw) const = 0;

  // Resets index then inserts entries read from @deserializer.
  virtual void recover(RecordReader& rr) = 0;

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

  // Updates hits information of a key.
  virtual void setHits(uint64_t key,
                       uint8_t currentHits,
                       uint8_t totalHits) = 0;

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
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

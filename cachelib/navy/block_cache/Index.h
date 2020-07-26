#pragma once

#include <cassert>
#include <memory>
#include <shared_mutex>
#include <utility>

#include <folly/Portability.h>
#include <folly/SharedMutex.h>
#include <tsl/sparse_map.h>

#include "cachelib/navy/serialization/RecordIO.h"

namespace facebook {
namespace cachelib {
namespace navy {
// NVM index: map from key to value. Under the hood, stores key hash to value
// map. If collision happened, returns undefined value (last inserted actually,
// but we do not want people to rely on that).
class Index {
 public:
  Index() = default;
  Index(const Index&) = delete;
  Index& operator=(const Index&) = delete;

  // Writes index to a Thrift object one bucket at a time and passes each bucket
  // to @persistCb. The reason for this is because the index can be very large
  // and serializing everything at once uses a lot of RAM.
  void persist(RecordWriter& rw) const;

  // Resets index then inserts entries read from @deserializer. Throws
  // std::exception on failure.
  void recover(RecordReader& rr);

  struct FOLLY_PACK_ATTR ItemRecord {
    uint32_t address{0};
    uint16_t size{0}; /* unused */
    uint8_t currentHits{0};
    uint8_t totalHits{0};
    ItemRecord(uint32_t _address = 0,
               uint16_t _size = 0,
               uint8_t _currentHits = 0,
               uint8_t _totalHits = 0)
        : address(_address),
          size(_size),
          currentHits(_currentHits),
          totalHits(_totalHits) {}
  };
  static_assert(8 == sizeof(ItemRecord), "ItemRecord size is 8 bytes");

  struct LookupResult {
    friend class Index;

    bool found() const { return found_; }

    ItemRecord record() const {
      XDCHECK(found_);
      return record_;
    }

    uint32_t address() const {
      XDCHECK(found_);
      return record_.address;
    }

    uint16_t size() const {
      XDCHECK(found_);
      return record_.size;
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
  };

  LookupResult lookup(uint64_t key) const;

  // Overwrites existing
  // If the entry was successfully overwritten, LookupResult.found() returns
  // true and LookupResult.record() returns the old record.
  LookupResult insert(uint64_t key, uint32_t address);

  // Replace old address with new address if there exists the key with the
  // identical old address. Return true if replaced.
  bool replaceIfMatch(uint64_t key, uint32_t newAddress, uint32_t oldAddress);

  // If the entry was successfully removed, LookupResult.found() returns true
  // and LookupResult.record() returns the record that was just found.
  // If the entry wasn't found, then LookupResult.found() returns false.
  LookupResult remove(uint64_t key);

  // Only remove if both key and address match.
  // Return true if removed successfully, false otherwise.
  bool removeIfMatch(uint64_t key, uint32_t address);

  void reset();

  // Walks buckets and computes total index entry count
  size_t computeSize() const;

 private:
  static constexpr uint32_t kNumBuckets{64 * 1024};
  static constexpr uint32_t kNumMutexes{1024};

  using Map = tsl::sparse_map<uint32_t, ItemRecord>;

  static uint32_t bucket(uint64_t hash) {
    return (hash >> 32) & (kNumBuckets - 1);
  }

  static uint32_t subkey(uint64_t hash) { return hash & 0xffffffffu; }

  folly::SharedMutex& getMutex(uint32_t bucket) const {
    XDCHECK(folly::isPowTwo(kNumMutexes));
    return mutex_[bucket & (kNumMutexes - 1)];
  }

  // Experiments with 64 byte alignment didn't show any throughput test
  // performance improvement.
  std::unique_ptr<folly::SharedMutex[]> mutex_{
      new folly::SharedMutex[kNumMutexes]};
  std::unique_ptr<Map[]> buckets_{new Map[kNumBuckets]};

  static_assert((kNumMutexes & (kNumMutexes - 1)) == 0,
                "number of mutexes must be power of two");
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

#pragma once

#include <cassert>
#include <memory>
#include <shared_mutex>
#include <utility>

#include <folly/SharedMutex.h>

#include "cachelib/navy/block_cache/BTree.h"
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

  struct LookupResult {
    friend class Index;

    bool found() const { return found_; }

    bool removed() const { return found(); }

    uint32_t value() const {
      XDCHECK(found_);
      return value_;
    }

   private:
    uint32_t value_{};
    bool found_{false};
  };
  LookupResult lookup(uint64_t key) const;

  // Overwrites existing
  // If the entry was successfully overwritten, LookupResult.found() returns true
  // and LookupResult.value() returns the address of the entry that was just
  // overwritten.
  LookupResult insert(uint64_t key, uint32_t value);

  // Replace old value with new value if there exists the key with the identical
  // old value. Return true if replaced.
  bool replace(uint64_t key, uint32_t newValue, uint32_t oldValue);

  // If the entry was successfully removed, LookupResult.found() returns true
  // and LookupResult.value() returns the address of the entry that was just
  // found.
  // If the entry wasn't found, then LookupResult.found() returns false.
  LookupResult remove(uint64_t key);

  // Only remove if both key and value match.
  // Return true if removed successfully, false otherwise.
  bool remove(uint64_t key, uint32_t value);

  void reset();

  // Walks buckets and computes total index entry count
  size_t computeSize() const;

 private:
  static constexpr uint32_t kNumBuckets{64 * 1024};
  static constexpr uint32_t kNumMutexes{1024};

  using Map = BTree<uint32_t, uint32_t, BTreeTraits<30, 60, 90>>;

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

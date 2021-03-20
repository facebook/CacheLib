#pragma once

#include <folly/Portability.h>

#include <functional>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/kangaroo/RripBucketStorage.h"
#include "cachelib/navy/kangaroo/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

// A bucket is the fundamental unit of read and write onto the device.
// On read, we read an entire bucket from device and then search for the key
// we need. On write, we read the entire bucket first to insert the new entry
// in memory and then write back to the device. Same for remove.
//
// To ensure the validity of a bucket, on reading, we first check if the
// checksum is what we expect. If it's unexpected, we will reinitialize
// the bucket, finish our operation, compute a new checksum, and finally
// store the checksum in the bucket. Checksum protects us from any device
// corruption. In addition, on first start-up, this is a convenient way
// to let us know a bucket had not been initialized.
//
// Each bucket has a generation timestamp associated with it. On reading, user
// must ensure the generation is what they expect. E.g. in BigHash, to trigger
// a ice roll, we'll update the global generation and then on next startup,
// we'll lazily invalidate each bucket as we read it as the generation will
// be a mismatch.
class FOLLY_PACK_ATTR RripBucket {
 public:
  // Iterator to bucket's items.
  class Iterator {
   public:
    bool done() const { return itr_.done(); }

    BufferView key() const;
    uint64_t keyHash() const;
    BufferView value() const;

    bool keyEqualsTo(HashedKey hk) const;
    bool keyEqualsTo(uint64_t keyHash) const;

   private:
    friend RripBucket;

    Iterator() = default;
    explicit Iterator(RripBucketStorage::Allocation itr) : itr_{itr} {}

    RripBucketStorage::Allocation itr_;
  };

  // User will pass in a view that contains the memory that is a RripBucket
  static uint32_t computeChecksum(BufferView view);

  // Initialize a brand new RripBucket given a piece of memory in the case
  // that the existing bucket is invalid. (I.e. checksum or generation
  // mismatch). Refer to comments at the top on what do we use checksum
  // and generation time for.
  static RripBucket& initNew(MutableBufferView view, uint64_t generationTime);

  uint32_t getChecksum() const { return checksum_; }

  void setChecksum(uint32_t checksum) { checksum_ = checksum; }

  uint64_t generationTime() const { return generationTime_; }

  uint32_t size() const { return storage_.numAllocations(); }

  // Look up for the value corresponding to a key.
  // BufferView::isNull() == true if not found.
  BufferView find(HashedKey hk, BitVectorUpdateVisitor setHitCallback) const;

  // HashedKey findKey(uint64_t key_hash) const;

  // Note: this does *not* replace an existing key! User must make sure to
  //       remove an existing key before calling insert.
  //
  // Insert into the bucket. Trigger eviction and invoke @destructorCb if
  // not enough space. Return number of entries evicted.
  uint32_t insert(HashedKey hk,
                  BufferView value,
                  uint8_t hits,
                  const DestructorCallback& destructorCb);

  // Remove an entry corresponding to the key. If found, invoke @destructorCb
  // before returning true. Return number of entries removed.
  // If not enough objects lower than
  uint32_t remove(HashedKey hk, const DestructorCallback& destructorCb);

  // Reorders entries in bucket based on RRIP
  void reorder(BitVectorReadVisitor isHitCallback);

  Iterator getFirst() const;
  Iterator getNext(Iterator itr) const;

  // Checks if there is space for an object given its hit priority
  // Use 0 hits if there is no log
  bool isSpace(HashedKey hk, BufferView value, uint8_t hits);

 private:
  RripBucket(uint64_t generationTime, uint32_t capacity)
      : generationTime_{generationTime}, storage_{capacity} {}

  // Reserve enough space for @size by evicting. Return number of evictions.
  uint32_t makeSpace(uint32_t size, const DestructorCallback& destructorCb);

  uint8_t getRripValue(uint8_t hits) const {
    uint8_t start = (1 << 3) - 2;
    if (hits > start) {
      return 0;
    }
    return start - hits;
  }

  uint8_t getIncrement(uint8_t highestRrip) const {
    uint8_t highestValue = (1 << 3) - 1;
    if (highestRrip > highestValue) {
      return 0;
    }
    return highestValue - highestRrip;
  }

  uint32_t checksum_{};
  uint64_t generationTime_{};
  RripBucketStorage storage_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

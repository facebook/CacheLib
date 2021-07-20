#pragma once

#include <folly/Portability.h>

#include "cachelib/navy/bighash/BucketStorage.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// BigHash is a series of buckets where each item is hashed to one of the
// buckets. A bucket is the fundamental unit of read and write onto the device.
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
class FOLLY_PACK_ATTR Bucket {
 public:
  // Iterator to bucket's items.
  class Iterator {
   public:
    // return whether the iteration has reached the end
    bool done() const { return itr_.done(); }

    BufferView key() const;
    uint64_t keyHash() const;
    BufferView value() const;

    bool keyEqualsTo(HashedKey hk) const;

   private:
    friend Bucket;

    Iterator() = default;
    explicit Iterator(BucketStorage::Allocation itr) : itr_{itr} {}

    BucketStorage::Allocation itr_;
  };

  // User will pass in a view that contains the memory that is a Bucket
  static uint32_t computeChecksum(BufferView view);

  // Initialize a brand new Bucket given a piece of memory in the case
  // that the existing bucket is invalid. (I.e. checksum or generation
  // mismatch). Refer to comments at the top on what do we use checksum
  // and generation time for.
  static Bucket& initNew(MutableBufferView view, uint64_t generationTime);

  uint32_t getChecksum() const { return checksum_; }

  void setChecksum(uint32_t checksum) { checksum_ = checksum; }

  // return the generation time of the bucket, if this is mismatch with
  // the one in BigHash data in the bucket is invalid.
  uint64_t generationTime() const { return generationTime_; }

  uint32_t size() const { return storage_.numAllocations(); }

  uint32_t remainingBytes() const { return storage_.remainingCapacity(); }

  // Look up for the value corresponding to a key.
  // BufferView::isNull() == true if not found.
  BufferView find(HashedKey hk) const;

  // Note: this does *not* replace an existing key! User must make sure to
  //       remove an existing key before calling insert.
  //
  // Insert into the bucket. Trigger eviction and invoke @destructorCb if
  // not enough space. Return number of entries evicted.
  uint32_t insert(HashedKey hk,
                  BufferView value,
                  const DestructorCallback& destructorCb);

  // Remove an entry corresponding to the key. If found, invoke @destructorCb
  // before returning true. Return number of entries removed.
  uint32_t remove(HashedKey hk, const DestructorCallback& destructorCb);

  // return an iterator of items in the bucket
  Iterator getFirst() const;
  Iterator getNext(Iterator itr) const;

 private:
  Bucket(uint64_t generationTime, uint32_t capacity)
      : generationTime_{generationTime}, storage_{capacity} {}

  // Reserve enough space for @size by evicting. Return number of evictions.
  uint32_t makeSpace(uint32_t size, const DestructorCallback& destructorCb);

  uint32_t checksum_{};
  uint64_t generationTime_{};
  BucketStorage storage_;
};

namespace details {
// This maps to exactly how an entry is stored in a bucket on device.
class FOLLY_PACK_ATTR BucketEntry {
 public:
  static uint32_t computeSize(uint32_t keySize, uint32_t valueSize) {
    return sizeof(BucketEntry) + keySize + valueSize;
  }

  // construct the BucketEntry with given memory using placement new
  // @param storage  the mutable memory used to create BucketEntry
  // @param hk       the item's key and its hash
  // @param value    the item's value
  static BucketEntry& create(MutableBufferView storage,
                             HashedKey hk,
                             BufferView value) {
    new (storage.data()) BucketEntry{hk, value};
    return reinterpret_cast<BucketEntry&>(*storage.data());
  }

  BufferView key() const { return {keySize_, data_}; }

  bool keyEqualsTo(HashedKey hk) const {
    return hk == HashedKey::precomputed(key(), keyHash_);
  }

  uint64_t keyHash() const { return keyHash_; }

  BufferView value() const { return {valueSize_, data_ + keySize_}; }

 private:
  BucketEntry(HashedKey hk, BufferView value)
      : keySize_{static_cast<uint32_t>(hk.key().size())},
        valueSize_{static_cast<uint32_t>(value.size())},
        keyHash_{hk.keyHash()} {
    static_assert(sizeof(BucketEntry) == 16, "BucketEntry overhead");
    hk.key().copyTo(data_);
    value.copyTo(data_ + keySize_);
  }

  const uint32_t keySize_{};
  const uint32_t valueSize_{};
  const uint64_t keyHash_{};
  uint8_t data_[];
};
} // namespace details
} // namespace navy
} // namespace cachelib
} // namespace facebook

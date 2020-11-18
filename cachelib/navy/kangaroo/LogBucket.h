#pragma once

#include <functional>

#include <folly/Portability.h>

#include "cachelib/navy/kangaroo/KangarooBucketStorage.h"
#include "cachelib/navy/kangaroo/Types.h"
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
class FOLLY_PACK_ATTR LogBucket {
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
    friend LogBucket;

    Iterator() = default;
    explicit Iterator(KangarooBucketStorage::Allocation itr) : itr_{itr} {}

    KangarooBucketStorage::Allocation itr_;
  };

  // User will pass in a view that contains the memory that is a KangarooBucket
  static uint32_t computeChecksum(BufferView view);

  // Initialize a brand new LogBucket given a piece of memory in the case
  // that the existing bucket is invalid. (I.e. checksum or generation
  // mismatch). Refer to comments at the top on what do we use checksum
  // and generation time for.
  static LogBucket& initNew(MutableBufferView view, uint64_t generationTime);

  uint32_t getChecksum() const { return checksum_; }

  void setChecksum(uint32_t checksum) { checksum_ = checksum; }

  uint64_t generationTime() const { return generationTime_; }

  uint32_t size() const { return storage_.numAllocations(); }

  // Look up for the value corresponding to a key.
  // BufferView::isNull() == true if not found.
  BufferView find(HashedKey hk) const;

  Status findTag(uint32_t tag, HashedKey& hk, BufferView& value) const;

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

  // Reorders entries in bucket based on NRU bit vector callback results
  void reorder(BitVectorReadVisitor isHitCallback);

  // Needed for log buckets, allocate does not remove objects
  bool isSpace(HashedKey hk, BufferView value);
  KangarooBucketStorage::Allocation allocate(HashedKey hk, BufferView value);
  void insert(KangarooBucketStorage::Allocation alloc, HashedKey hk, BufferView value);
  void clear();

  Iterator getFirst() const;
  Iterator getNext(Iterator itr) const;

 private:
  LogBucket(uint64_t generationTime, uint32_t capacity)
      : generationTime_{generationTime}, storage_{capacity} {}

  // Reserve enough space for @size by evicting. Return number of evictions.
  uint32_t makeSpace(uint32_t size, const DestructorCallback& destructorCb);

  uint32_t checksum_{};
  uint64_t generationTime_{};
  KangarooBucketStorage storage_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

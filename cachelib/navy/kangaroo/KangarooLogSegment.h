#pragma once

#include <stdexcept>

#include <folly/SharedMutex.h>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/LogBucket.h"
#include "cachelib/navy/kangaroo/LogIndex.h"

namespace facebook {
namespace cachelib {
namespace navy {
class KangarooLogSegment  {
 public:
  class Iterator {
   public:
    bool done() const { return done_; }

    HashedKey key() const { return HashedKey(itr_.key()); }

    BufferView value() const { return itr_.value(); }

   private:
    friend KangarooLogSegment;

    explicit Iterator(uint64_t bucketNum, LogBucket::Iterator itr) : 
      bucketNum_{bucketNum}, itr_{itr} {
      if (itr_.done()) {
        done_ = true;
      }
    }

    uint64_t bucketNum_;
    LogBucket::Iterator itr_;
    bool done_ = false;
  };

  explicit KangarooLogSegment(uint64_t segmentSize, uint64_t pageSize, 
      LogSegmentId lsid, uint64_t pagesPerPartition, 
      MutableBufferView mutableView, bool newBucket);

  ~KangarooLogSegment() {delete buckets_;}

  KangarooLogSegment(const KangarooLogSegment&) = delete;
  KangarooLogSegment& operator=(const KangarooLogSegment&) = delete;

  // Look up for the value corresponding to a key.
  // BufferView::isNull() == true if not found.
  BufferView find(HashedKey hk, LogPageId lpid);
  BufferView findTag(uint32_t tag, HashedKey& hk, LogPageId lpid);

  // Insert into the segment. Returns invalid page id if there is no room.
  LogPageId insert(HashedKey hk, BufferView value);

  LogSegmentId getLogSegmentId();

  void clear(LogSegmentId newLsid);

  Iterator getFirst();
  Iterator getNext(Iterator itr);

 private:
  uint32_t bucketOffset(LogPageId lpid);
  LogPageId getLogPageId(uint32_t bucketOffset);
  
  // allocation on Kangaroo Bucket lock 
  folly::SharedMutex allocationMutex_;
  
  uint64_t segmentSize_;
  uint64_t pageSize_;
  uint64_t numBuckets_;
  uint64_t pagesPerPartition_;
  LogSegmentId lsid_;
  // pointer to array of pointers to LogBuckets
  LogBucket** buckets_; 
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

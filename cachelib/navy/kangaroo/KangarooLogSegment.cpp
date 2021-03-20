#include "cachelib/navy/kangaroo/KangarooBucketStorage.h"
#include "cachelib/navy/kangaroo/KangarooLogSegment.h"

namespace facebook {
namespace cachelib {
namespace navy {

KangarooLogSegment::KangarooLogSegment(uint64_t segmentSize, 
      uint64_t pageSize, LogSegmentId lsid, uint64_t pagesPerPartition,
      MutableBufferView mutableView, bool newBucket)
  : segmentSize_{segmentSize},
    pageSize_{pageSize},
    numBuckets_{segmentSize_ / pageSize_},
    pagesPerPartition_{pagesPerPartition},
    lsid_{lsid},
    buckets_{new LogBucket*[numBuckets_]} {
  // initialize all of the Kangaroo Buckets after cast
  for (uint64_t i = 0; i < numBuckets_; i++) {
    // TODO: fix generation time
    uint64_t offset = i * pageSize_;
    auto view = MutableBufferView(pageSize_, mutableView.data() + offset);
    if (newBucket) {
      LogBucket::initNew(view, 0);
    }
    buckets_[i] = reinterpret_cast<LogBucket*>(mutableView.data() + offset);
  }
}

BufferView KangarooLogSegment::find(HashedKey hk, LogPageId lpid) {
  uint32_t offset = bucketOffset(lpid);
  XDCHECK(offset < numBuckets_);
  return buckets_[offset]->find(hk);
}

BufferView KangarooLogSegment::findTag(uint32_t tag, HashedKey& hk, LogPageId lpid) {
  uint32_t offset = bucketOffset(lpid);
  XDCHECK(offset < numBuckets_);
  return  buckets_[offset]->findTag(tag, hk);
}

LogPageId KangarooLogSegment::insert(HashedKey hk, BufferView value) {
  KangarooBucketStorage::Allocation alloc;
  uint32_t i = 0;
  bool foundAlloc = false;
  {
    std::unique_lock<folly::SharedMutex> lock{allocationMutex_};
    // not necessarily the best online bin packing heuristic
    // could potentially also do better sharding which segment
    // to choose for performance reasons depending on bottleneck
    for (; i < numBuckets_;  i++) {
      if (buckets_[i]->isSpace(hk, value)) {
        alloc = buckets_[i]->allocate(hk, value);
        foundAlloc = true;
        break;
      }
    }
  }
  if (!foundAlloc) {
    return LogPageId(0, false);
  }
  // space already reserved so no need to hold mutex
  buckets_[i]->insert(alloc, hk, value);
  return getLogPageId(i);
}
  
uint32_t KangarooLogSegment::bucketOffset(LogPageId lpid) {
  return lpid.index() % numBuckets_;
}

LogPageId KangarooLogSegment::getLogPageId(uint32_t bucketOffset) {
  return LogPageId(lsid_.partition() * pagesPerPartition_ 
      + numBuckets_ * lsid_.index() + bucketOffset, true);
}

LogSegmentId KangarooLogSegment::getLogSegmentId() {
  return lsid_;
}

void KangarooLogSegment::clear(LogSegmentId newLsid) {
  lsid_ = newLsid;
  for (uint64_t i = 0; i < numBuckets_; i++) {
    buckets_[i]->clear();
  }
}
  
KangarooLogSegment::Iterator KangarooLogSegment::getFirst() {
  uint64_t bucketNum = 0;
  auto itr = buckets_[bucketNum]->getFirst();
  return Iterator(bucketNum, itr);
}

KangarooLogSegment::Iterator KangarooLogSegment::getNext(Iterator itr) {
  if (itr.done()) {
    return itr;
  }
  auto nextItr = buckets_[itr.bucketNum_]->getNext(itr.itr_);
  if (nextItr.done() && itr.bucketNum_ >= numBuckets_ - 1) {
    itr.done_ = true;
    return itr;
  } else if (nextItr.done()) {
    itr.bucketNum_++;
    itr.itr_ = buckets_[itr.bucketNum_]->getFirst();
    return itr;
  } else {
    itr.itr_ = nextItr;
    return itr;
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

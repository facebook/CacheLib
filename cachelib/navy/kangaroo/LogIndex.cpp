#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/kangaroo/LogIndex.h"

namespace facebook {
namespace cachelib {
namespace navy {

LogIndex::LogIndex(uint64_t numSlots, SetNumberCallback setNumberCb) 
    : numSlots_{numSlots},
      setNumberCb_{setNumberCb} {
  index_ = new LogIndexEntry[numSlots];
  for (uint64_t i = 0; i < numSlots_; i++) {
    index_[i].hits_ = 0;
    index_[i].valid_ = 0;
  }
}

LogIndex::~LogIndex() {
  delete index_;
}

LogPageId LogIndex::lookup(HashedKey hk, bool hit) {
  const uint32_t offset = getLogIndexOffset(hk);
  uint32_t increment = 0;
  uint32_t tag = createTag(hk);
  LogIndexEntry* currentHead = &index_[(offset + increment) % numSlots_];
  while (currentHead->continueIteration() && increment < numSlots_) {
    if (currentHead->isValid() && 
        currentHead->tag() == tag) {
      if (hit) {
        currentHead->incrementHits();
      }
      return currentHead->page();
    }
    increment++;
    currentHead = &index_[(offset + increment) % numSlots_];
  }
  return LogPageId(0, false);
}

Status LogIndex::insert(HashedKey hk, LogPageId lpid, uint8_t hits) {
  const auto offset = getLogIndexOffset(hk); 
  uint32_t increment = 0;
  uint32_t tag = createTag(hk);
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  while (increment < numSlots_) {
    if (entry->tag() == tag || !entry->isValid()) {
      Status ret;
      if (entry->isValid()) {
      	ret = Status::NotFound;
      } else {
	ret = Status::Ok;
      }
      entry->tag_ = tag;
      entry->flash_index_ = lpid.index();
      entry->valid_ = 1;
      entry->hits_ = hits;
      return ret;
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return Status::Rejected;
}

Status LogIndex::remove(HashedKey hk, LogPageId lpid) {
  const auto offset = getLogIndexOffset(hk); 
  uint32_t increment = 0;
  uint32_t tag = createTag(hk);
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  while (increment < numSlots_) {
    if (entry->isValid() && entry->tag() == tag && lpid == entry->page()) {
      entry->invalidate();
      if (!index_[(offset + increment + 1) % numSlots_].continueIteration()) {
        entry->clear();
      }
      return Status::Ok;
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return Status::NotFound;
}

// Counts number of items in log corresponding to bucket 
uint64_t LogIndex::countBucket(HashedKey hk) {
  const auto offset = getLogIndexOffset(hk); 
  uint32_t increment = 0;
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  uint64_t count = 0;
  while (increment < numSlots_) {
    if (!entry->continueIteration()) {
      break;
    } else if (entry->isValid()) {
      count++;
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return count;
}

// Get iterator for all items in the same bucket
LogIndex::BucketIterator LogIndex::getHashBucketIterator(HashedKey hk) {
  const auto offset = getLogIndexOffset(hk); 
  uint32_t increment = 0;
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  auto idx = setNumberCb_(hk.keyHash());
  while (increment < numSlots_) {
    if (!entry->continueIteration()) {
      break;
    } else if (entry->isValid()) {
      return BucketIterator(idx, entry, increment);
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return BucketIterator();
}
  
LogIndex::BucketIterator LogIndex::getNext(LogIndex::BucketIterator bi) {
  auto offset = getLogIndexOffsetFromSetBucket(bi.bucket_);
  uint32_t increment = bi.increment_ + 1;
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  while (increment < numSlots_) {
    if (!entry->continueIteration()) {
      break;
    } else if (entry->isValid()) {
      return BucketIterator(bi.bucket_, entry, increment);
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return BucketIterator();
}

LogPageId LogIndex::findAndRemove(KangarooBucketId bid, uint32_t tag) {
  auto offset = getLogIndexOffsetFromSetBucket(bid);
  uint32_t increment = 0;
  LogIndexEntry* entry = &index_[(offset + increment) % numSlots_];
  while (increment < numSlots_) {
    if (entry->isValid() && entry->tag() == tag) {
      LogPageId lpid = entry->page();
      entry->invalidate();
      if (!index_[(offset + increment + 1) % numSlots_].continueIteration()) {
        entry->clear();
      }
      return lpid;
    }
    increment++;
    entry = &index_[(offset + increment) % numSlots_];
  }
  return LogPageId(0, false);
}

uint32_t LogIndex::getLogIndexOffset(HashedKey hk) {
  return getLogIndexOffsetFromSetBucket(setNumberCb_(hk.keyHash()));
}

uint32_t LogIndex::getLogIndexOffset(uint64_t key) {
  return getLogIndexOffsetFromSetBucket(setNumberCb_(key));
}

uint32_t LogIndex::getLogIndexOffsetFromSetBucket(KangarooBucketId bid) {
  return bid.index() % numSlots_;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

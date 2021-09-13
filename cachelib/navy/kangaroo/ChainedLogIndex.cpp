#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/kangaroo/ChainedLogIndex.h"

namespace facebook {
namespace cachelib {
namespace navy {

void ChainedLogIndex::allocate() {
  {
    numAllocations_++;
    allocations.resize(numAllocations_);
    allocations[numAllocations_ - 1] = new ChainedLogIndexEntry[allocationSize_];
  }
}

ChainedLogIndex::ChainedLogIndex(uint64_t numHashBuckets,
        uint16_t allocationSize, SetNumberCallback setNumberCb) 
    : numHashBuckets_{numHashBuckets},
      allocationSize_{allocationSize},
      numMutexes_{numHashBuckets / 10 + 1},
      setNumberCb_{setNumberCb} {
  mutexes_ = std::make_unique<folly::SharedMutex[]>(numMutexes_);
  index_.resize(numHashBuckets_, -1);
  {
    std::unique_lock<folly::SharedMutex> lock{allocationMutex_};
    allocate();
  }
}

ChainedLogIndex::~ChainedLogIndex() {
  {
    std::unique_lock<folly::SharedMutex> lock{allocationMutex_};
    /*for (uint64_t i = 0; i < numAllocations_; i++) {
      delete allocations[i];
    }*/
  }
}

ChainedLogIndexEntry* ChainedLogIndex::findEntryNoLock(uint16_t offset) {
  uint16_t arrayOffset = offset % allocationSize_;
  uint16_t vectorOffset = offset / allocationSize_;
  if (vectorOffset > numAllocations_) {
    return nullptr;
  }
  return &allocations[vectorOffset][arrayOffset];
}

ChainedLogIndexEntry* ChainedLogIndex::findEntry(uint16_t offset) {
  std::shared_lock<folly::SharedMutex> lock{allocationMutex_};
  return findEntryNoLock(offset);
}

ChainedLogIndexEntry* ChainedLogIndex::allocateEntry(uint16_t& offset) {
  std::unique_lock<folly::SharedMutex> lock{allocationMutex_};
  if (nextEmpty_ >= numAllocations_ * allocationSize_) {
    allocate();
  }
  offset = nextEmpty_;
  ChainedLogIndexEntry* entry = findEntryNoLock(offset);
  if (nextEmpty_ == maxSlotUsed_) {
    nextEmpty_++;
    maxSlotUsed_++;
  } else {
    nextEmpty_ = entry->next_;
  }
  entry->next_ = -1;
  return entry;
}

uint16_t ChainedLogIndex::releaseEntry(uint16_t offset) {
  std::unique_lock<folly::SharedMutex> lock{allocationMutex_};
  ChainedLogIndexEntry* entry = findEntryNoLock(offset);
  uint16_t ret = entry->next_;
  entry->invalidate();
  entry->next_ = nextEmpty_;
  nextEmpty_ = offset;
  return ret;
}

PartitionOffset ChainedLogIndex::lookup(HashedKey hk, bool hit, uint32_t* hits) {
  const auto lib = getLogIndexBucket(hk); 
  uint32_t tag = createTag(hk);
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(lib)};
    ChainedLogIndexEntry* currentHead = findEntry(index_[lib.index()]);
    while (currentHead) {
      if (currentHead->isValid() && 
          currentHead->tag() == tag) {
        if (hit) {
          currentHead->incrementHits();
        }
        if (hits != nullptr) {
          *hits = currentHead->hits();
        }
        return currentHead->offset();
      }
      currentHead = findEntry(currentHead->next());
    }
  }
  hits = 0;
  return PartitionOffset(0, false);
}

Status ChainedLogIndex::insert(HashedKey hk, PartitionOffset po, uint8_t hits) {
  const auto lib = getLogIndexBucket(hk); 
  uint32_t tag = createTag(hk);
  insert(tag, lib, po, hits);
}

Status ChainedLogIndex::insert(uint32_t tag, KangarooBucketId bid,
    PartitionOffset po, uint8_t hits) {
  const auto lib = getLogIndexBucketFromSetBucket(bid);
  insert(tag, lib, po, hits);
}

Status ChainedLogIndex::insert(uint32_t tag, LogIndexBucket lib, 
    PartitionOffset po, uint8_t hits) {
  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(lib)};

    uint16_t* oldNext = &index_[lib.index()];
    ChainedLogIndexEntry* nextEntry = findEntry(index_[lib.index()]);
    while (nextEntry) {
      if (nextEntry->isValid() && nextEntry->tag() == tag) {
        nextEntry->populateEntry(po, tag, hits);
        return Status::Ok;
      }
      oldNext = &nextEntry->next_;
      nextEntry = findEntry(*oldNext);
    }

    uint16_t entryOffset;
    ChainedLogIndexEntry* newEntry = allocateEntry(entryOffset);
    newEntry->populateEntry(po, tag, hits);
    (*oldNext) = entryOffset;
  }
  return Status::Ok;
}

Status ChainedLogIndex::remove(HashedKey hk, PartitionOffset po) {
  uint64_t tag = createTag(hk);
  const auto lib = getLogIndexBucket(hk); 
  return remove(tag, lib, po);
}
  
Status ChainedLogIndex::remove(uint64_t tag, KangarooBucketId bid, PartitionOffset po) {
  auto lib = getLogIndexBucketFromSetBucket(bid);
  return remove(tag, lib, po);
}

Status ChainedLogIndex::remove(uint64_t tag, LogIndexBucket lib, PartitionOffset po) {
  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(lib)};
    ChainedLogIndexEntry* nextEntry = findEntry(index_[lib.index()]);
    uint16_t* oldNext = &index_[lib.index()];
    while (nextEntry) {
      if (nextEntry->isValid() && nextEntry->tag() == tag && nextEntry->offset() == po) {
        *oldNext = releaseEntry(*oldNext);
        return Status::Ok;
      }
      oldNext = &nextEntry->next_;
      nextEntry = findEntry(nextEntry->next_);
    }
  }
  return Status::NotFound;
}

// Counts number of items in log corresponding to bucket 
uint64_t ChainedLogIndex::countBucket(HashedKey hk) {
  const auto lib = getLogIndexBucket(hk); 
  uint64_t count = 0;
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(lib)};
    ChainedLogIndexEntry* nextEntry = findEntry(index_[lib.index()]);
    while (nextEntry) {
      if (nextEntry->isValid()) {
        count++;
      }
      nextEntry = findEntry(nextEntry->next_);
    }
  }
  return count;
}

// Get iterator for all items in the same bucket
ChainedLogIndex::BucketIterator ChainedLogIndex::getHashBucketIterator(HashedKey hk) {
  const auto lib = getLogIndexBucket(hk); 
  auto idx = setNumberCb_(hk.keyHash());
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(lib)};
    auto currentHead = findEntry(index_[lib.index()]);
    while (currentHead) {
      if (currentHead->isValid()) {
        return BucketIterator(idx, currentHead);
      }
      currentHead = findEntry(currentHead->next_);
    }
  }
  return BucketIterator();
}
  
ChainedLogIndex::BucketIterator ChainedLogIndex::getNext(ChainedLogIndex::BucketIterator bi) {
  if (bi.done()) {
    return bi;
  }
  auto lib = getLogIndexBucketFromSetBucket(bi.bucket_);
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(lib)};
    auto currentHead = findEntry(bi.nextEntry_);
    while (currentHead) {
      if (currentHead->isValid()) {
        return BucketIterator(bi.bucket_, currentHead);
      }
      currentHead = findEntry(currentHead->next_);
    }
  }
  return BucketIterator();
}

PartitionOffset ChainedLogIndex::find(KangarooBucketId bid, uint64_t tag) {
  auto lib = getLogIndexBucketFromSetBucket(bid);
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(lib)};
    ChainedLogIndexEntry* nextEntry = findEntry(index_[lib.index()]);
    uint16_t* oldNext = &index_[lib.index()];
    while (nextEntry) {
      if (nextEntry->isValid() && nextEntry->tag() == tag) {
        PartitionOffset po = nextEntry->offset();
        return po;
      }
      oldNext = &nextEntry->next_;
      nextEntry = findEntry(nextEntry->next_);
    }
  }
  return PartitionOffset(0, false);
}

ChainedLogIndex::LogIndexBucket ChainedLogIndex::getLogIndexBucket(HashedKey hk) {
  return getLogIndexBucketFromSetBucket(setNumberCb_(hk.keyHash()));
}

ChainedLogIndex::LogIndexBucket ChainedLogIndex::getLogIndexBucket(uint64_t key) {
  return getLogIndexBucketFromSetBucket(setNumberCb_(key));
}

ChainedLogIndex::LogIndexBucket ChainedLogIndex::getLogIndexBucketFromSetBucket(KangarooBucketId bid) {
  return LogIndexBucket(bid.index() % numHashBuckets_);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

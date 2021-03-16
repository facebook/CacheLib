#pragma once

#include <vector>

#include <folly/SharedMutex.h>

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/ChainedLogIndexEntry.h"

namespace facebook {
namespace cachelib {
namespace navy {
// ChainedLogIndex is a hash-based log index optimized for allowing easy 
// threshold lookups
//
// It primarily is a chained hash table that chains so that 
// items in the same on-flash Kangaroo bucket will end up in the 
// same hash bucket in the index. This way we avoid scans to
// see if items can end up in the same set.
class ChainedLogIndex  {
 public:
  // BucketIterator gives hashed key for each valid
  // element corresponding to a given kangaroo bucket
  // Read only
  class BucketIterator {
   public:
    BucketIterator() : end_{true} {}

    bool done() const { return end_; }

    uint32_t tag() const { return tag_; }

    uint32_t hits() const { return hits_; }

    PartitionOffset offset() const { return offset_; }

   private:
    friend ChainedLogIndex;

    BucketIterator(KangarooBucketId id, ChainedLogIndexEntry* firstKey) 
      : bucket_{id}, tag_{firstKey->tag()}, hits_{firstKey->hits()},
        offset_{firstKey->offset()}, nextEntry_{firstKey->next_} {}

    KangarooBucketId bucket_{0};
    uint32_t tag_;
    uint32_t hits_;
    PartitionOffset offset_{0, 0};
    uint16_t nextEntry_;
    bool end_{false};
  };
  
  explicit ChainedLogIndex(uint64_t numHashBuckets, 
          uint16_t allocationSize, SetNumberCallback setNumberCb);

  ~ChainedLogIndex();

  ChainedLogIndex(const ChainedLogIndex&) = delete;
  ChainedLogIndex& operator=(const ChainedLogIndex&) = delete;

  // Look up a key in Index. 
  // If not found, return will not be valid.
  PartitionOffset lookup(HashedKey hk, bool hit, uint32_t* hits);

  // Inserts key into index. 
  Status insert(HashedKey hk, PartitionOffset po, uint8_t hits = 0);
  Status insert(uint32_t tag, KangarooBucketId bid, PartitionOffset po, uint8_t hits);

  // Removes entry's valid bit if it's in the log 
  Status remove(HashedKey hk, PartitionOffset lpid);
  Status remove(uint64_t tag, KangarooBucketId bid, PartitionOffset lpid);

  // does not create a hit, for log flush lookups
  PartitionOffset find(KangarooBucketId bid, uint64_t tag);

  // Counts number of items in log corresponding to set 
  // bucket for the hashed key
  uint64_t countBucket(HashedKey hk);

  // Get iterator for all items in the same bucket
  BucketIterator getHashBucketIterator(HashedKey hk);
  BucketIterator getNext(BucketIterator bi);

 private:

  friend BucketIterator;
  
  class LogIndexBucket {
   public:
    explicit LogIndexBucket(uint32_t idx) : idx_{idx} {}

    bool operator==(const LogIndexBucket& rhs) const noexcept {
      return idx_ == rhs.idx_;
    }
    bool operator!=(const LogIndexBucket& rhs) const noexcept {
      return !(*this == rhs);
    }

    uint32_t index() const noexcept { return idx_; }

   private:
    uint32_t idx_;
  };
  
  Status remove(uint64_t tag, LogIndexBucket lib, PartitionOffset lpid);
  Status insert(uint32_t tag, LogIndexBucket lib, PartitionOffset po, uint8_t hits);

  LogIndexBucket getLogIndexBucket(HashedKey hk);
  LogIndexBucket getLogIndexBucket(uint64_t hk);
  LogIndexBucket getLogIndexBucketFromSetBucket(KangarooBucketId bid);

  // locks based on log index hash bucket, concurrent read, single modify
  folly::SharedMutex& getMutex(LogIndexBucket lib) const {
    return mutexes_[lib.index() & (numMutexes_ - 1)];
  }

  const uint64_t numMutexes_{};
  const uint64_t numHashBuckets_{};
  const SetNumberCallback setNumberCb_{};
  std::unique_ptr<folly::SharedMutex[]> mutexes_;
  std::vector<uint16_t> index_;

  folly::SharedMutex allocationMutex_;
  const uint16_t allocationSize_{};
  uint16_t maxSlotUsed_{0};
  uint16_t nextEmpty_{0};
  uint16_t numAllocations_{0};

  void allocate();
  ChainedLogIndexEntry* findEntry(uint16_t offset);
  ChainedLogIndexEntry* findEntryNoLock(uint16_t offset);
  ChainedLogIndexEntry* allocateEntry(uint16_t& offset);
  uint16_t releaseEntry(uint16_t offset);

  std::vector<ChainedLogIndexEntry*> allocations;

};
} // namespace navy
} // namespace cachelib
} // namespace facebook



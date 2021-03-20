#pragma once

#include <folly/SharedMutex.h>

#include <vector>

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/LogIndexEntry.h"
#include "cachelib/navy/kangaroo/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// LogIndex is an open-addressing hash-based log index
// optimized for allowing easy threshold lookups.
// It uses linear probing.
//
// Requires user to handle synchronization.
class LogIndex {
 public:
  // BucketIterator gives hashed key for each valid
  // element corresponding to a given kangaroo bucket
  // Read only
  class BucketIterator {
   public:
    BucketIterator()
        : bucket_{0}, current_entry_{nullptr}, increment_{0}, end_{true} {}

    bool done() const { return end_; }

    uint32_t hits() const { return current_entry_->hits(); }

    LogPageId page() const { return current_entry_->page(); }

    uint32_t tag() const { return current_entry_->tag(); }

   private:
    friend LogIndex;

    BucketIterator(KangarooBucketId id,
                   LogIndexEntry* firstKey,
                   uint32_t increment)
        : bucket_{id}, current_entry_{firstKey}, increment_{increment} {}

    KangarooBucketId bucket_;
    LogIndexEntry* current_entry_;
    uint32_t increment_;
    bool end_{false};
  };

  explicit LogIndex(uint64_t numSlots, SetNumberCallback setNumberCb);

  ~LogIndex();

  LogIndex(const LogIndex&) = delete;
  LogIndex& operator=(const LogIndex&) = delete;

  // Look up a key in Index.
  // If not found, return will not be valid.
  LogPageId lookup(HashedKey hk, bool hit);

  // Inserts key into index. Will reject the request
  // if index has no room
  Status insert(HashedKey hk, LogPageId lpid, uint8_t hits = 0);

  // Removes entry's valid bit if it's in the log
  Status remove(HashedKey hk, LogPageId lpid);

  LogPageId findAndRemove(KangarooBucketId bid, uint32_t tag);

  // Counts number of items in log corresponding to set
  // bucket for the hashed key
  uint64_t countBucket(HashedKey hk);

  // Get iterator for all items in the same bucket
  BucketIterator getHashBucketIterator(HashedKey hk);
  BucketIterator getNext(BucketIterator bi);

 private:
  friend BucketIterator;

  uint32_t getLogIndexOffset(HashedKey hk);
  uint32_t getLogIndexOffset(uint64_t hk);
  uint32_t getLogIndexOffsetFromSetBucket(KangarooBucketId bid);

  LogIndexEntry* index_;
  uint64_t numSlots_;
  const SetNumberCallback setNumberCb_{};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

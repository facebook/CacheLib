#pragma once 

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/kangaroo/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ChainedLogIndex;

class __attribute__((__packed__)) ChainedLogIndexEntry {
 public:
  ChainedLogIndexEntry() : valid_{false} {}
  ~ChainedLogIndexEntry() = default;

  bool operator==(const ChainedLogIndexEntry& rhs) const noexcept {
    return valid_ && rhs.valid_ && tag_ == rhs.tag_;
  }
  bool operator!=(const ChainedLogIndexEntry& rhs) const noexcept {
    return !(*this == rhs);
  }

  void populateEntry(PartitionOffset po, uint32_t tag, uint8_t hits) {
    flash_index_ = po.index();
    tag_ = tag;
    valid_ = 1;
    hits_ = hits;
  }

  void incrementHits() { if (hits_ < ((1 << 3) - 1)) {hits_++;} }
  uint32_t hits() { return hits_; }
  uint32_t tag() { return tag_; }
  void invalidate() { valid_ = 0; }
  bool isValid() { return valid_; }
  PartitionOffset offset() { return PartitionOffset(flash_index_, valid_); }
  uint16_t next() { return next_; }

 private:
  friend ChainedLogIndex;

  uint32_t flash_index_ : 19;
  uint32_t tag_ : 9;
  uint32_t valid_ : 1;
  uint32_t hits_ : 3;
  uint16_t next_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

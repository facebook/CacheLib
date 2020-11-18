#pragma once 

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class LogIndex;

class LogIndexEntry {
 public:
  explicit LogIndexEntry(HashedKey hk, LogPageId lpid)
    : tag_{createTag(hk)}, flash_index_{lpid.index()}, hits_{0} {}
  LogIndexEntry() : valid_{false} {}
  ~LogIndexEntry() = default;

  bool operator==(const LogIndexEntry& rhs) const noexcept {
    return valid_ && rhs.valid_ && tag_ == rhs.tag_;
  }
  bool operator!=(const LogIndexEntry& rhs) const noexcept {
    return !(*this == rhs);
  }

  void incrementHits() { if (hits_ < (1 << 3 - 1)) {hits_++;} }
  uint32_t hits() { return hits_; }
  uint32_t tag() { return tag_; }
  void invalidate() { valid_ = 0; hits_ = 1; }
  void clear() { hits_ = 0; valid_ = 0; }
  bool isValid() { return valid_; }
  bool continueIteration() { return isValid() || hits_; }
  LogPageId page() { return LogPageId(flash_index_, valid_); }
    
 private:
  friend LogIndex;

  uint32_t tag_ : 9;
  uint32_t flash_index_ : 19;
  uint32_t valid_ : 1;
  uint32_t hits_ : 3;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

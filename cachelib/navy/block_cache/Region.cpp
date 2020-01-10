#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
void Region::reset() {
  XDCHECK_EQ(activeOpen(), 0U);
  classId_ = kClassIdMax;
  flags_ = 0;
  activeReaders_ = 0;
  activeWriters_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
}

RegionDescriptor Region::open(OpenMode mode) {
  if (flags_ & kBlockAccess) {
    return RegionDescriptor{OpenStatus::Retry};
  }
  switch (mode) {
  case OpenMode::Write:
    activeWriters_++;
    break;
  case OpenMode::Read:
    activeReaders_++;
    break;
  default:
    XDCHECK(false);
  }
  return RegionDescriptor{OpenStatus::Ready, regionId_, mode};
}

void Region::close(RegionDescriptor&& desc) {
  switch (desc.mode()) {
  case OpenMode::Write:
    activeWriters_--;
    break;
  case OpenMode::Read:
    activeReaders_--;
    break;
  default:
    XDCHECK(false);
  }
}

bool Region::tryLock() {
  if ((flags_ & kBlockAccess) != 0 && activeOpen() == 0) {
    auto saveFlags = flags_;
    flags_ |= kLock;
    return flags_ != saveFlags;
  } else {
    return false;
  }
}

RelAddress Region::allocate(uint32_t size) {
  if (lastEntryEndOffset_ + size <= regionSize_) {
    auto offset = lastEntryEndOffset_;
    lastEntryEndOffset_ += size;
    numItems_++;
    return RelAddress{regionId_, offset};
  } else {
    throw std::logic_error("can not allocate");
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

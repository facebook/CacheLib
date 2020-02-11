#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
bool Region::readyForReclaim() {
  flags_ |= kBlockAccess;
  return activeOpen() == 0;
}

uint32_t Region::activeOpen() { return activeReaders_ + activeWriters_; }

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(
    uint32_t size) {
  XDCHECK(!(flags_ & kBlockAccess));
  if (!canAllocate(size)) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, RelAddress{});
  }
  activeWriters_++;
  return std::make_tuple(
      RegionDescriptor{OpenStatus::Ready, regionId_, OpenMode::Write},
      allocate(size));
}

RegionDescriptor Region::openForRead() {
  if (flags_ & kBlockAccess) {
    // Region is currently in reclaim, retry later
    return RegionDescriptor{OpenStatus::Retry};
  }
  activeReaders_++;
  return RegionDescriptor{OpenStatus::Ready, regionId_, OpenMode::Read};
}

void Region::reset() {
  XDCHECK_EQ(activeOpen(), 0U);
  classId_ = kClassIdMax;
  flags_ = 0;
  activeReaders_ = 0;
  activeWriters_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
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

RelAddress Region::allocate(uint32_t size) {
  XDCHECK(canAllocate(size));
  auto offset = lastEntryEndOffset_;
  lastEntryEndOffset_ += size;
  numItems_++;
  return RelAddress{regionId_, offset};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

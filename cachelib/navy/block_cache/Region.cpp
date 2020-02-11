#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
bool Region::readyForReclaim() {
  std::lock_guard<std::mutex> l{lock_};
  flags_ |= kBlockAccess;
  return activeOpenLocked() == 0;
}

uint32_t Region::activeOpenLocked() { return activeReaders_ + activeWriters_; }

std::tuple<RegionDescriptor, RelAddress> Region::openAndAllocate(
    uint32_t size) {
  std::lock_guard<std::mutex> l{lock_};
  XDCHECK(!(flags_ & kBlockAccess));
  if (!canAllocateLocked(size)) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, RelAddress{});
  }
  activeWriters_++;
  return std::make_tuple(
      RegionDescriptor{OpenStatus::Ready, regionId_, OpenMode::Write},
      allocateLocked(size));
}

RegionDescriptor Region::openForRead() {
  std::lock_guard<std::mutex> l{lock_};
  if (flags_ & kBlockAccess) {
    // Region is currently in reclaim, retry later
    return RegionDescriptor{OpenStatus::Retry};
  }
  activeReaders_++;
  return RegionDescriptor{OpenStatus::Ready, regionId_, OpenMode::Read};
}

void Region::reset() {
  std::lock_guard<std::mutex> l{lock_};
  XDCHECK_EQ(activeOpenLocked(), 0U);
  classId_ = kClassIdMax;
  flags_ = 0;
  activeReaders_ = 0;
  activeWriters_ = 0;
  lastEntryEndOffset_ = 0;
  numItems_ = 0;
}

void Region::close(RegionDescriptor&& desc) {
  std::lock_guard<std::mutex> l{lock_};
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

RelAddress Region::allocateLocked(uint32_t size) {
  XDCHECK(canAllocateLocked(size));
  auto offset = lastEntryEndOffset_;
  lastEntryEndOffset_ += size;
  numItems_++;
  return RelAddress{regionId_, offset};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

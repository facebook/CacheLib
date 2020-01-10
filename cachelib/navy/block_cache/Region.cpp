#include "cachelib/navy/block_cache/Region.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Open region for access. Returns false if failed (retry later).
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
} // namespace navy
} // namespace cachelib
} // namespace facebook

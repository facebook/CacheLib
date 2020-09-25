#include <cassert>
#include <utility>

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/block_cache/Allocator.h"

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

void RegionAllocator::setAllocationRegion(RegionId rid) {
  XDCHECK(!rid_.valid());
  rid_ = rid;
}

void RegionAllocator::reset() { rid_ = RegionId{}; }

Allocator::Allocator(RegionManager& regionManager)
    : regionManager_{regionManager} {
  const auto& sizeClasses = regionManager_.getSizeClasses();
  if (sizeClasses.size() > Region::kClassIdMax + 1) {
    throw std::invalid_argument{"too many size classes"};
  }
  if (sizeClasses.empty()) {
    XLOG(INFO, "Allocator type: stack");
    allocators_.emplace_back(0 /* classId */);
  } else {
    XLOG(INFO, "Allocator type: size classes");
    for (size_t i = 0; i < sizeClasses.size(); i++) {
      allocators_.emplace_back(i /* classId */);
    }
  }
}

uint32_t Allocator::getSlotSizeAndClass(uint32_t size, uint32_t& sc) const {
  const auto& sizeClasses = regionManager_.getSizeClasses();
  sc = 0;
  if (sizeClasses.empty()) {
    return size;
  } else {
    auto it = std::lower_bound(sizeClasses.begin(), sizeClasses.end(), size);
    if (it == sizeClasses.end()) {
      return 0;
    }
    sc = it - sizeClasses.begin();
    return *it;
  }
}

std::tuple<RegionDescriptor, uint32_t, RelAddress> Allocator::allocate(
    uint32_t size) {
  uint32_t sc = 0;
  size = getSlotSizeAndClass(size, sc);
  RegionAllocator* ra = &allocators_[sc];

  if (size == 0 || size > regionManager_.regionSize()) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, size,
                           RelAddress());
  }
  return allocateWith(*ra, size);
}

// Allocates using region allocator @ra. If region is full, we take another
// from the clean list (regions ready for allocation) If the clean list is
// empty, we retry allocation. This means reclamation doesn't keep up and we
// have to wait. Every time we take a region from the clean list, we schedule
// new reclamation job to refill it. Caller must close the region after data
// written to the slot.
std::tuple<RegionDescriptor, uint32_t, RelAddress> Allocator::allocateWith(
    RegionAllocator& ra, uint32_t size) {
  LockGuard l{ra.getLock()};
  RegionId rid = ra.getAllocationRegion();
  if (rid.valid()) {
    auto& region = regionManager_.getRegion(rid);
    auto [desc, addr] = region.openAndAllocate(size);
    XDCHECK_NE(OpenStatus::Reclaimed, desc.status());
    XDCHECK_NE(OpenStatus::Retry, desc.status());
    if (desc.isReady()) {
      return std::make_tuple(std::move(desc), size, addr);
    }
    XDCHECK_EQ(OpenStatus::Error, desc.status());
    // Buffer has been fully allocated. Release the region by scheduling an
    // async flush to flush all in-mem writes, and reset the region allocator
    // to get ready to get a new region
    flushAndReleaseRegionFromRALocked(ra, true /* async */);
    rid = RegionId{};
  }

  // if we are here, we either didn't find a valid region or the region we
  // picked ended up being full.
  XDCHECK(!rid.valid());
  auto status = regionManager_.getCleanRegion(rid);
  if (status != OpenStatus::Ready) {
    return std::make_tuple(RegionDescriptor{status}, size, RelAddress{});
  }

  // we got a region fresh off of reclaim. Need to initialize it.
  auto& region = regionManager_.getRegion(rid);

  region.setClassId(ra.classId());

  // Replace with a reclaimed region and allocate
  ra.setAllocationRegion(rid);
  auto [desc, addr] = region.openAndAllocate(size);
  XDCHECK_EQ(OpenStatus::Ready, desc.status());
  return std::make_tuple(std::move(desc), size, addr);
}

void Allocator::close(RegionDescriptor&& desc) {
  regionManager_.close(std::move(desc));
}

void Allocator::flushAndReleaseRegionFromRALocked(RegionAllocator& ra,
                                                  bool flushAsync) {
  auto rid = ra.getAllocationRegion();
  if (rid.valid()) {
    regionManager_.doFlush(rid, flushAsync);
    ra.reset();
  }
}

void Allocator::flush() {
  if (!regionManager_.doesBufferingWrites()) {
    return;
  }
  for (auto& ra : allocators_) {
    std::lock_guard<std::mutex> lock{ra.getLock()};
    flushAndReleaseRegionFromRALocked(ra, false /* async */);
  }
}

void Allocator::reset() {
  regionManager_.reset();
  for (auto& ra : allocators_) {
    std::lock_guard<std::mutex> lock{ra.getLock()};
    ra.reset();
  }
}

void Allocator::getCounters(const CounterVisitor& visitor) const {
  regionManager_.getCounters(visitor);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

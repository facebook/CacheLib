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

Allocator::Allocator(RegionManager& regionManager, uint32_t blockSize)
    : regionManager_{regionManager},
      blockSize_{blockSize},
      permItemAllocator_{0 /* classId */} {
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

uint32_t Allocator::alignOnBlock(uint32_t size) const {
  size = powTwoAlign(size, blockSize_);
  if (size > regionManager_.regionSize()) {
    return 0;
  }
  return size;
}

uint32_t Allocator::getSlotSizeAndClass(uint32_t size, uint32_t& sc) const {
  const auto& sizeClasses = regionManager_.getSizeClasses();
  sc = 0;
  if (sizeClasses.empty()) {
    return alignOnBlock(size);
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
    uint32_t size, bool permanent) {
  RegionAllocator* ra = nullptr;
  if (permanent) {
    ra = &permItemAllocator_;
    size = alignOnBlock(size);
  } else {
    uint32_t sc = 0;
    size = getSlotSizeAndClass(size, sc);
    ra = &allocators_[sc];
  }
  if (size == 0) {
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
  RelAddress addr;
  LockGuard l{ra.getLock()};
  RegionId rid = ra.getAllocationRegion();
  // region is full, track it and reset ra and get a new region
  if (rid.valid()) {
    std::lock_guard<std::mutex> rlock{regionManager_.getLock(rid)};
    auto& region = regionManager_.getRegion(rid);
    auto desc = region.open(OpenMode::Write);
    if (!desc.isReady()) {
      return std::make_tuple(std::move(desc), size, addr);
    }
    bool canAllocate = region.canAllocate(size);
    if (canAllocate) {
      addr = region.allocate(size);
      return std::make_tuple(std::move(desc), size, addr);
    } else {
      if (!region.isPinned()) {
        regionManager_.track(rid);
      }
      ra.reset();
      region.close(std::move(desc));
      rid = RegionId{};
    }
  }

  // if we are here, we either didn't find a valid region or the region we
  // picked ended up being full.
  XDCHECK(!rid.valid());
  auto status = regionManager_.getCleanRegion(rid);
  if (status != OpenStatus::Ready) {
    return std::make_tuple(RegionDescriptor{status}, size, addr);
  }

  // we got a region fresh off of reclaim. Need to initialize it.
  auto& region = regionManager_.getRegion(rid);
  LockGuard rlock{regionManager_.getLock(rid)};
  if (isPermanentAllocator(ra)) {
    // Pin immediately. We want to persist this region as pinned even if it
    // is not full.
    regionManager_.pin(region);
    XDCHECK(region.isPinned());
  } else {
    region.setClassId(ra.classId());
  }

  // Replace with a reclaimed region
  ra.setAllocationRegion(rid);
  auto desc = region.open(OpenMode::Write);
  if (!desc.isReady()) {
    return std::make_tuple(std::move(desc), size, addr);
  }

  XDCHECK(region.canAllocate(size));
  addr = region.allocate(size);
  return std::make_tuple(std::move(desc), size, addr);
}

void Allocator::close(RegionDescriptor&& desc) {
  regionManager_.close(std::move(desc));
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

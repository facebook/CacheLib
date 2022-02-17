/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachelib/navy/block_cache/Allocator.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cassert>
#include <utility>

#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

void RegionAllocator::setAllocationRegion(RegionId rid) {
  XDCHECK(!rid_.valid());
  rid_ = rid;
}

void RegionAllocator::reset() { rid_ = RegionId{}; }

Allocator::Allocator(RegionManager& regionManager, uint16_t numPriorities)
    : regionManager_{regionManager} {
  const auto& sizeClasses = regionManager_.getSizeClasses();
  if (sizeClasses.size() > Region::kClassIdMax + 1) {
    throw std::invalid_argument{"too many size classes"};
  }

  auto createAllocators = [](uint16_t numClasses, uint16_t numPris) {
    XLOGF(INFO,
          "Enable priority-based allocation for Allocator. Number of "
          "priorities: {}",
          numPris);
    std::vector<std::vector<RegionAllocator>> allocators;
    for (uint16_t i = 0; i < numClasses; i++) {
      std::vector<RegionAllocator> priAllocators;
      for (uint16_t j = 0; j < numPris; j++) {
        priAllocators.emplace_back(i /* classId */, j /* priority */);
      }
      allocators.push_back(std::move(priAllocators));
    }
    return allocators;
  };

  if (sizeClasses.empty()) {
    XLOG(INFO, "Allocator type: stack");
    allocators_ = createAllocators(1, numPriorities);
  } else {
    XLOGF(INFO, "Allocator type: size classes: {}", sizeClasses.size());
    allocators_ = createAllocators(static_cast<uint16_t>(sizeClasses.size()),
                                   numPriorities);
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
    uint32_t size, uint16_t priority) {
  uint32_t sc = 0;
  size = getSlotSizeAndClass(size, sc);
  XDCHECK_LT(priority, allocators_[sc].size());
  RegionAllocator* ra = &allocators_[sc][priority];
  if (size == 0 || size > regionManager_.regionSize()) {
    return std::make_tuple(RegionDescriptor{OpenStatus::Error}, size,
                           RelAddress());
  }
  return allocateWith(*ra, size);
} // namespace cachelib

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
  region.setPriority(ra.priority());
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
  for (auto& ras : allocators_) {
    for (auto& ra : ras) {
      std::lock_guard<std::mutex> lock{ra.getLock()};
      flushAndReleaseRegionFromRALocked(ra, false /* async */);
    }
  }
}

void Allocator::reset() {
  regionManager_.reset();
  for (auto& ras : allocators_) {
    for (auto& ra : ras) {
      std::lock_guard<std::mutex> lock{ra.getLock()};
      ra.reset();
    }
  }
}

void Allocator::getCounters(const CounterVisitor& visitor) const {
  regionManager_.getCounters(visitor);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

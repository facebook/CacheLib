/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
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

#include <folly/logging/xlog.h>

#include <utility>

#include "cachelib/navy/common/NavyThread.h"

namespace facebook::cachelib::navy {

void RegionAllocator::setAllocationRegion(RegionId rid) {
  XDCHECK(!rid_.valid());
  rid_ = rid;
}

void RegionAllocator::reset() { rid_ = RegionId{}; }

Allocator::Allocator(RegionManager& regionManager,
                     const std::vector<uint32_t>& allocatorsPerPriority)
    : regionManager_{regionManager} {
  size_t numPriorities = allocatorsPerPriority.size();
  XLOGF(INFO,
        "Enable priority-based allocation for Allocator. Number of "
        "priorities: {}",
        numPriorities);
  for (uint16_t i = 0; i < numPriorities; i++) {
    allocators_.emplace_back();
    for (uint32_t j = 0; j < allocatorsPerPriority[i]; j++) {
      allocators_.back().emplace_back(i /* priority */);
    }
  }
}

std::pair<RegionDescriptor, RelAddress> Allocator::allocate(uint32_t size,
                                                            uint16_t priority,
                                                            bool canWait,
                                                            uint64_t keyHash) {
  XDCHECK_LT(priority, allocators_.size());
  if (size == 0 || size > regionManager_.regionSize()) {
    return std::make_pair(RegionDescriptor{OpenStatus::Error}, RelAddress());
  }
  RegionAllocator* ra =
      &allocators_[priority][keyHash % allocators_[priority].size()];
  return allocateWith(*ra, size, canWait);
} // namespace cachelib

// Allocates using region allocator @ra. If region is full, we take another
// from the clean list (regions ready for allocation) If the clean list is
// empty, we retry allocation. This means reclaim doesn't keep up and we
// have to wait. Every time we take a region from the clean list, we schedule
// new reclaim job to refill it. Caller must close the region after data
// written to the slot.
std::pair<RegionDescriptor, RelAddress> Allocator::allocateWith(
    RegionAllocator& ra, uint32_t size, bool canWait) {
  std::unique_lock lock{ra.getLock()};
  RegionId rid = ra.getAllocationRegion();
  if (rid.valid()) {
    auto& region = regionManager_.getRegion(rid);
    auto [desc, addr] = region.openAndAllocate(size);
    XDCHECK_NE(OpenStatus::Retry, desc.status());
    if (desc.isReady()) {
      return std::make_pair(std::move(desc), addr);
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

  if (canWait && !isOnNavyThread()) {
    // Waiting on main thread could cause indefinite blocking, so do not wait
    canWait = false;
  }

  auto [status, waiter] = regionManager_.getCleanRegion(rid, canWait);
  if (status == OpenStatus::Retry) {
    lock.unlock();
    if (waiter) {
      allocRetryWaits_.inc();
      waiter->baton_.wait();
    }
    return std::make_pair(RegionDescriptor{status}, RelAddress{});
  }
  XDCHECK(status == OpenStatus::Ready);
  XDCHECK(!waiter);

  // we got a region fresh off of reclaim. Need to initialize it.
  auto& region = regionManager_.getRegion(rid);
  region.setPriority(ra.priority());

  // Replace with a reclaimed region and allocate
  ra.setAllocationRegion(rid);
  auto [desc, addr] = region.openAndAllocate(size);
  XDCHECK_EQ(OpenStatus::Ready, desc.status());
  return std::make_pair(std::move(desc), addr);
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
      std::lock_guard lock{ra.getLock()};
      flushAndReleaseRegionFromRALocked(ra, false /* async */);
    }
  }
}

void Allocator::reset() {
  allocRetryWaits_.set(0);
  regionManager_.reset();
  for (auto& ras : allocators_) {
    for (auto& ra : ras) {
      std::lock_guard lock{ra.getLock()};
      ra.reset();
    }
  }
}

void Allocator::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_alloc_retries_waits", allocRetryWaits_.get(),
          CounterVisitor::CounterType::RATE);
  regionManager_.getCounters(visitor);
}

} // namespace facebook::cachelib::navy

#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/block_cache/RegionManager.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Class to allocate a particular size from a region. Not thread safe, caller
// has to sync access.
class RegionAllocator {
 public:
  // @param classId   size class this region allocator is associated with
  // @param priority  priority this region allocator is associated with
  explicit RegionAllocator(uint16_t classId, uint16_t priority)
      : classId_{classId}, priority_{priority} {}

  RegionAllocator(const RegionAllocator&) = delete;
  RegionAllocator& operator=(const RegionAllocator&) = delete;
  RegionAllocator(RegionAllocator&& other) noexcept
      : classId_{other.classId_},
        priority_{other.priority_},
        rid_{other.rid_} {}

  // Sets new region to allocate from. Region allocator has to be reset before
  // calling this.
  void setAllocationRegion(RegionId rid);

  // Returns the current allocation region unique ID.
  RegionId getAllocationRegion() const { return rid_; }

  // Resets allocator to the inital state.
  void reset();

  // Returns the size class this region allocator is associated with.
  uint16_t classId() const { return classId_; }

  // Returns the priority this region allocator is associated with.
  uint16_t priority() const { return priority_; }

  // Returns the mutex lock.
  std::mutex& getLock() const { return mutex_; }

 private:
  const uint16_t classId_{};
  const uint16_t priority_{};

  // The current region id from which we are allocating
  RegionId rid_;

  mutable std::mutex mutex_;
};

// Size class or stack allocator. Thread safe. Syncs access
class Allocator {
 public:
  // Constructs an allocator.
  //
  // @param regionManager     Used to get eviction information and for
  //                          locking regions
  // @param numPriorities     Specifies how many priorities this allocator
  //                          supports
  // Throws std::exception if invalid arguments
  explicit Allocator(RegionManager& regionManager, uint16_t numPriorities);

  // Checks whether size class is used.
  // Returns true if @sizeClasses is not empty; false otherwise.
  bool isSizeClassAllocator() const {
    return regionManager_.getSizeClasses().size() > 0;
  }

  // Allocates and opens for writing.
  //
  // @param size          Allocation size
  // @param priority      Specifies how important this allocation is
  //
  // Returns a tuple containing region descriptor, allocated slotSize and
  // allocated address
  // The region descriptor contains region id, open mode and status,
  // which is one of the following
  //  - Ready   Fills @addr and @slotSize
  //  - Retry   Retry later, reclamation is running
  //  - Error   Can't allocate this size even later (hard failure)
  // When allocating with a priority, the priority must NOT exceed the
  // max priority which is (@numPriorities - 1) specified when constructing
  // this allocator.
  std::tuple<RegionDescriptor, uint32_t, RelAddress> allocate(
      uint32_t size, uint16_t priority);

  // Closes the region.
  void close(RegionDescriptor&& rid);

  // Resets the region to the initial state.
  void reset();

  // Flushes any regions with in-memory buffers to device.
  void flush();

  // Exports Allocator stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const;

 private:
  using LockGuard = std::lock_guard<std::mutex>;
  Allocator(const Allocator&) = delete;
  Allocator& operator=(const Allocator&) = delete;

  // Releases region associated with the region allocator by flushing the
  // in-memory buffers and resetting the ra.
  void flushAndReleaseRegionFromRALocked(RegionAllocator& ra, bool flushAsync);

  // Allocates @size bytes in region allocator @ra. If succeed (enough space),
  // returns region descriptor, size and address.
  std::tuple<RegionDescriptor, uint32_t, RelAddress> allocateWith(
      RegionAllocator& ra, uint32_t size);

  uint32_t getSlotSizeAndClass(uint32_t size, uint32_t& sc) const;

  RegionManager& regionManager_;
  // Corresponding RegionAllocators (see regionManager_.sizeClasses_)
  std::vector<std::vector<RegionAllocator>> allocators_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

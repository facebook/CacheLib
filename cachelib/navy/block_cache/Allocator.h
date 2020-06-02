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
  explicit RegionAllocator(uint32_t classId) : classId_{classId} {}

  RegionAllocator(const RegionAllocator&) = delete;
  RegionAllocator& operator=(const RegionAllocator&) = delete;
  RegionAllocator(RegionAllocator&& other) noexcept
      : classId_{other.classId_}, rid_{other.rid_} {}
  RegionAllocator& operator=(RegionAllocator&& other) = default;

  // Set new region to allocate from. Region allocator has to be reset before
  // calling this.
  void setAllocationRegion(RegionId rid);

  RegionId getAllocationRegion() const { return rid_; }

  // Resets allocator (releases used region)
  void reset();

  uint32_t classId() const { return classId_; }

  std::mutex& getLock() const { return mutex_; }

 private:
  const uint32_t classId_{};

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
  // Throws std::exception if invalid arguments
  explicit Allocator(RegionManager& regionManager);

  bool isSizeClassAllocator() const {
    return regionManager_.getSizeClasses().size() > 0;
  }

  // Allocates and opens for writing.
  //
  // @param size              Allocation size
  // @param permanent         Indicates if the allocation is permanent or not
  //
  // Returns a tuple containing region descriptor, allocated slotSize and
  // allocated address
  // The region descriptor contains region id, open mode and status,
  // which is one of the following
  //  - Ready   Fills @addr and @slotSize
  //  - Retry   Retry later, reclamation is running
  //  - Error   Can't allocate this size even later (hard failure)
  std::tuple<RegionDescriptor, uint32_t, RelAddress> allocate(uint32_t size,
                                                              bool permanent);
  void close(RegionDescriptor&& rid);

  void reset();

  // Flushes any regions with in-memory buffers to device
  void flush();

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

  bool isPermanentAllocator(const RegionAllocator& ra) const {
    return &ra == &permItemAllocator_;
  }

  RegionManager& regionManager_;

  // Corresponding RegionAllocators (see regionManager_.sizeClasses_)
  std::vector<RegionAllocator> allocators_;
  RegionAllocator permItemAllocator_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

#pragma once

#include <mutex>

#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
enum class OpenMode {
  // Not opened
  None,

  // Write Mode, caller always uses this mode for writing
  Write,

  // Read Mode, caller always uses this mode for reading
  Read,

  // Physical Read Mode. Read Mode is modified to Phys Read Mode if
  // inMemBuffers are in use and the buffer is flushed.
  PhysRead,
};

enum class OpenStatus {
  // Caller can proceed
  Ready,

  // Sequence number mismatch
  Retry,

  // Error (like too large, out of regions)
  Error,

  // Region is being reclaimed
  Reclaimed,
};

class RegionDescriptor;

// Region. Has optional client provided classId which is used to map from
// region to allocator (or other structure). Responsible for open/lock
// synchronization and keeps count of active readers/writers.
class Region {
 public:
  static constexpr uint32_t kClassIdMax{(1u << 16) - 1};

  Region(RegionId id, uint64_t regionSize)
      : regionId_{id}, regionSize_{regionSize} {}

  Region(const serialization::Region& d, RegionId rid, uint64_t regionSize)
      : regionId_{rid},
        regionSize_{regionSize},
        classId_{static_cast<uint16_t>(d.classId)},
        lastEntryEndOffset_{static_cast<uint32_t>(d.lastEntryEndOffset)},
        numItems_{static_cast<uint32_t>(d.numItems)} {}

  // Disable copy constructor to avoid mistakes like below:
  //   auto r = RegionManager.getRegion(rid);
  // Did you notice that we forgot reference "&"?
  Region(const Region&) = delete;
  Region& operator=(const Region&) = delete;

  // Immediately block future accesses to this region. Return true if
  // there are no pending operation to this region, false otherwise.
  // It is safe to repeatedly call this until success. Note that it is
  // only safe for one thread to call this, as we assume only a single
  // thread can be running region reclaim at a time.
  bool readyForReclaim();

  // Open this region for write and allocate a slot of @size.
  // Fail if there's insufficient space.
  std::tuple<RegionDescriptor, RelAddress> openAndAllocate(uint32_t size);

  // Open this region for reading. Fail if region is blocked,
  RegionDescriptor openForRead();

  // Reset the region's internal state. This is used to reset state
  // after a region has been reclaimed.
  void reset();

  // Close the region and consume the region descriptor.
  void close(RegionDescriptor&& desc);

  // Associate this region with a RegionAllocator
  void setClassId(uint16_t classId) {
    XDCHECK(!isPinned());
    classId_ = classId;
  }
  uint16_t getClassId() const {
    XDCHECK(!isPinned());
    return classId_;
  }

  // Set this region as pinned (i.e. non-evictable)
  void setPinned() { flags_ |= kPinned; }
  bool isPinned() const { return (flags_ & kPinned) != 0; }

  uint32_t getLastEntryEndOffset() const { return lastEntryEndOffset_; }

  uint32_t getNumItems() const { return numItems_; }

 private:
  uint32_t activeOpen();

  uint32_t canAllocate(uint32_t size) {
    return (lastEntryEndOffset_ + size <= regionSize_);
  }

  RelAddress allocate(uint32_t size);

  static constexpr uint32_t kBlockAccess{1u << 0};
  static constexpr uint16_t kPinned{1u << 1};

  const RegionId regionId_{};
  const uint64_t regionSize_{0};

  uint16_t classId_{kClassIdMax};
  uint16_t flags_{0};
  uint32_t activeReaders_{0};
  uint32_t activeWriters_{0};
  // End offset of last slot added to region
  uint32_t lastEntryEndOffset_{0};
  uint32_t numItems_{0};
};

// RegionDescriptor. Contains status of the open, region id and the open mode.
// This is returned when a open is done in read/write mode. close() uses the
// descriptor to properly close and update the internal counters.
class RegionDescriptor {
 public:
  explicit RegionDescriptor(OpenStatus status,
                            RegionId regionId = RegionId{},
                            OpenMode mode = OpenMode::None)
      : status_(status), regionId_(regionId), mode_(mode) {}

  RegionDescriptor(const RegionDescriptor&) = delete;
  RegionDescriptor& operator=(const RegionDescriptor&) = delete;

  RegionDescriptor(RegionDescriptor&& o) noexcept
      : status_{o.status_}, regionId_{o.regionId_}, mode_{o.mode_} {
    o.mode_ = OpenMode::None;
    o.regionId_ = RegionId{};
  }

  RegionDescriptor& operator=(RegionDescriptor&& o) noexcept {
    if (this != &o) {
      this->~RegionDescriptor();
      new (this) RegionDescriptor(std::move(o));
    }
    return *this;
  }

  bool isReady() { return status_ == OpenStatus::Ready; }

  OpenMode mode() { return mode_; }

  OpenStatus status() { return status_; }

  RegionId id() { return regionId_; }

 private:
  OpenStatus status_;
  RegionId regionId_{};
  OpenMode mode_{OpenMode::None};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

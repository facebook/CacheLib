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

  void reset();

  RegionDescriptor open(OpenMode mode);
  void close(RegionDescriptor&& desc);

  void setClassId(uint16_t classId) {
    XDCHECK(!isPinned());
    classId_ = classId;
  }

  bool isPinned() const { return (flags_ & kPinned) != 0; }

  uint16_t getClassId() const {
    XDCHECK(!isPinned());
    return classId_;
  }

  // Block access. Succeding @open will fail until unblocked.
  bool blockAccess() {
    flags_ |= kBlockAccess;
    return activeOpen() == 0;
  }

  void unblockAccess() { flags_ &= ~kBlockAccess; }

  // Try set lock bit on. Returns true if set and the caller got exclusive
  // access. Access block and lock are orthogonal concepts, but typically we
  // want to block particular access and then set the lock bit.
  bool tryLock();

  void unlock() { flags_ &= ~kLock; }

  uint32_t getLastEntryEndOffset() const { return lastEntryEndOffset_; }

  uint32_t getNumItems() const { return numItems_; }

  uint32_t canAllocate(uint32_t size) {
    return (lastEntryEndOffset_ + size <= regionSize_);
  }

  RelAddress allocate(uint32_t size);

  void setPinned() { flags_ |= kPinned; }

 private:
  uint32_t activeOpen() { return activeReaders_ + activeWriters_; }
  static constexpr uint32_t kBlockAccess{1u << 0};
  static constexpr uint32_t kLock{1u << 1};
  static constexpr uint16_t kPinned{1u << 2};

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

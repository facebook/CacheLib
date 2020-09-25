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
    std::lock_guard<std::mutex> l{lock_};
    classId_ = classId;
  }
  uint16_t getClassId() const {
    std::lock_guard<std::mutex> l{lock_};
    return classId_;
  }

  uint32_t getLastEntryEndOffset() const {
    std::lock_guard<std::mutex> l{lock_};
    return lastEntryEndOffset_;
  }

  uint32_t getNumItems() const {
    std::lock_guard<std::mutex> l{lock_};
    return numItems_;
  }

  // If this region is actively used, then the fragmentation
  // is the bytes at the end of the region that's not used.
  uint32_t getFragmentationSize() const {
    std::lock_guard<std::mutex> l{lock_};
    if (numItems_) {
      return regionSize_ - lastEntryEndOffset_;
    }
    return 0;
  }

  // Write buf to attached buffer at offset 'offset'
  void writeToBuffer(uint32_t offset, BufferView buf);

  // Read from attached buffer from 'fromOffset' into 'outBuf'
  void readFromBuffer(uint32_t fromOffset, MutableBufferView outBuf) const;

  // Attach buffer 'buf' to the region
  void attachBuffer(std::unique_ptr<Buffer>&& buf) {
    std::lock_guard l{lock_};
    XDCHECK_EQ(buffer_, nullptr);
    buffer_ = std::move(buf);
  }

  // checks if the region has buffer attached
  bool hasBuffer() const {
    std::lock_guard l{lock_};
    return buffer_.get() != nullptr;
  }

  // detaches the attached buffer and returns it only if there are no
  // active readers, otherwise returns nullptr
  std::unique_ptr<Buffer> detachBuffer() {
    std::lock_guard l{lock_};
    XDCHECK_NE(buffer_, nullptr);
    if (activeInMemReaders_ == 0) {
      XDCHECK_EQ(activeWriters_, 0UL);
      auto retBuf = std::move(buffer_);
      buffer_ = nullptr;
      return retBuf;
    }
    return nullptr;
  }

  // flushes the attached buffer by calling the callBack function.
  // The callBack function is expected to write to the underlying device.
  // The callback function should return true if successfully flushed the
  // buffer, otherwise it should return false.
  bool flushBuffer(std::function<bool(RelAddress, BufferView)> callBack);

  void setPendingFlush() {
    std::lock_guard l{lock_};
    XDCHECK_NE(buffer_, nullptr);
    XDCHECK((flags_ & (kFlushPending | kFlushed)) == 0);

    flags_ |= kFlushPending;
  }

  // checks if the region's buffer is flushed
  bool isFlushedLocked() const { return (flags_ & kFlushed) != 0; }

  // returns the number of active writers using the region
  uint32_t getActiveWriters() const {
    std::lock_guard l{lock_};
    return activeWriters_;
  }

  // returns the number of active readers using the region
  uint32_t getActiveInMemReaders() const {
    std::lock_guard l{lock_};
    return activeInMemReaders_;
  }

  // returns the region id
  RegionId id() const { return regionId_; }

 private:
  uint32_t activeOpenLocked();

  // checks to see if there is enough space in the region for a new write of
  // size 'size'
  bool canAllocateLocked(uint32_t size) const {
    // assert that buffer is not flushed and flush is not pending
    XDCHECK((flags_ & (kFlushPending | kFlushed)) == 0);
    return (lastEntryEndOffset_ + size <= regionSize_);
  }

  RelAddress allocateLocked(uint32_t size);

  static constexpr uint32_t kBlockAccess{1u << 0};
  static constexpr uint16_t kPinned{1u << 1};
  static constexpr uint16_t kFlushPending{1u << 2};
  static constexpr uint16_t kFlushed{1u << 3};

  const RegionId regionId_{};
  const uint64_t regionSize_{0};

  uint16_t classId_{kClassIdMax};
  uint16_t flags_{0};
  uint32_t activePhysReaders_{0};
  uint32_t activeInMemReaders_{0};
  uint32_t activeWriters_{0};
  // End offset of last slot added to region
  uint32_t lastEntryEndOffset_{0};
  uint32_t numItems_{0};
  std::unique_ptr<Buffer> buffer_{nullptr};

  mutable std::mutex lock_;
};

// RegionDescriptor. Contains status of the open, region id and the open mode.
// This is returned when a open is done in read/write mode. close() uses the
// descriptor to properly close and update the internal counters.
class RegionDescriptor {
 public:
  RegionDescriptor(OpenStatus status) : status_(status) {}
  static RegionDescriptor makeWriteDescriptor(OpenStatus status,
                                              RegionId regionId) {
    return RegionDescriptor{status, regionId, OpenMode::Write};
  }
  static RegionDescriptor makeReadDescriptor(OpenStatus status,
                                             RegionId regionId,
                                             bool physReadMode) {
    return RegionDescriptor{status, regionId, OpenMode::Read, physReadMode};
  }
  RegionDescriptor(const RegionDescriptor&) = delete;
  RegionDescriptor& operator=(const RegionDescriptor&) = delete;

  RegionDescriptor(RegionDescriptor&& o) noexcept
      : status_{o.status_},
        regionId_{o.regionId_},
        mode_{o.mode_},
        physReadMode_{o.physReadMode_} {
    o.mode_ = OpenMode::None;
    o.regionId_ = RegionId{};
    o.status_ = OpenStatus::Retry;
    o.physReadMode_ = false;
  }

  RegionDescriptor& operator=(RegionDescriptor&& o) noexcept {
    if (this != &o) {
      this->~RegionDescriptor();
      new (this) RegionDescriptor(std::move(o));
    }
    return *this;
  }

  bool isPhysReadMode() const {
    return (mode_ == OpenMode::Read) && physReadMode_;
  }

  bool isReady() const { return status_ == OpenStatus::Ready; }

  OpenMode mode() const { return mode_; }

  OpenStatus status() const { return status_; }

  RegionId id() const { return regionId_; }

 private:
  RegionDescriptor(OpenStatus status,
                   RegionId regionId,
                   OpenMode mode,
                   bool physReadMode = false)
      : status_(status),
        regionId_(regionId),
        mode_(mode),
        physReadMode_(physReadMode) {}

  OpenStatus status_;
  RegionId regionId_{};
  OpenMode mode_{OpenMode::None};
  // physReadMode_ is applicable only in read mode
  bool physReadMode_{false};
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

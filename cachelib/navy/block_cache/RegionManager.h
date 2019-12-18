#pragma once

#include <cassert>
#include <memory>
#include <mutex>
#include <utility>

#include <folly/container/F14Map.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/serialization/RecordIO.h"
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
      : classId_{static_cast<uint16_t>(d.classId)},
        lastEntryEndOffset_{static_cast<uint32_t>(d.lastEntryEndOffset)},
        numItems_{static_cast<uint32_t>(d.numItems)},
        regionId_{rid},
        regionSize_{regionSize} {}

  // Disable copy constructor to avoid mistakes like below:
  //   auto r = RegionManager.getRegion(rid);
  // Did you notice that we forgot reference "&"?
  Region(const Region&) = delete;
  Region& operator=(const Region&) = delete;

  RegionDescriptor open(OpenMode mode);
  void close(RegionDescriptor&& desc);

  void reset() {
    XDCHECK_EQ(activeOpen(), 0U);
    classId_ = kClassIdMax;
    flags_ = 0;
    activeReaders_ = 0;
    activeWriters_ = 0;
    lastEntryEndOffset_ = 0;
    numItems_ = 0;
  }

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
  bool tryLock() {
    if ((flags_ & kBlockAccess) != 0 && activeOpen() == 0) {
      auto saveFlags = flags_;
      flags_ |= kLock;
      return flags_ != saveFlags;
    } else {
      return false;
    }
  }

  void unlock() { flags_ &= ~kLock; }

  uint32_t getLastEntryEndOffset() const { return lastEntryEndOffset_; }

  uint32_t getNumItems() const { return numItems_; }

  uint32_t canAllocate(uint32_t size) {
    return (lastEntryEndOffset_ + size <= regionSize_);
  }

  RelAddress allocate(uint32_t size) {
    if (lastEntryEndOffset_ + size <= regionSize_) {
      auto offset = lastEntryEndOffset_;
      lastEntryEndOffset_ += size;
      numItems_++;
      return RelAddress{regionId_, offset};
    } else {
      throw std::logic_error("can not allocate");
    }
  }

  void setPinned() { flags_ |= kPinned; }

 private:
  uint32_t activeOpen() { return activeReaders_ + activeWriters_; }
  static constexpr uint32_t kBlockAccess{1u << 0};
  static constexpr uint32_t kLock{1u << 1};
  static constexpr uint16_t kPinned{1u << 2};

  uint16_t classId_{kClassIdMax};
  uint16_t flags_{0};
  uint32_t activeReaders_{0};
  uint32_t activeWriters_{0};
  // End offset of last slot added to region
  uint32_t lastEntryEndOffset_{0};
  uint32_t numItems_{0};
  const RegionId regionId_{};
  const uint64_t regionSize_{0};
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

// Callback that is used to clear index.
//   @rid       Region ID
//   @buffer    Buffer with region data, valid during callback invocation
//   @slotSize  Region slot size (0 for stack allocator)
// Returns number of slots evicted
using RegionEvictCallback =
    std::function<uint32_t(RegionId rid, uint32_t slotSize, BufferView buffer)>;

// Size class or stack allocator. Thread safe. Syncs access, reclaims regions
// Controls the allocation of regions, status (open for read/write), and
// eviction. Region manager doesn't have internal locks. External caller must
// take care of locking.
class RegionManager {
 public:
  // constructs a Region Manager
  //
  // @param numRegions        number of regions
  // @param regionSize        size of the region
  // @param baseOffset        base offset of the region
  // @param blockSize         block size
  // @param device            reference to device
  // @param numCleanRegions   How many regions reclamator maintains in
  //                          the clean pool
  // @param scheduler         JobScheduler to run reclamation jobs
  // @param regionEvictCb     Callback invoked when region evicted
  // @param policy            eviction policy
  RegionManager(uint32_t numRegions,
                uint64_t regionSize,
                uint64_t baseOffset,
                uint32_t blockSize,
                Device& device,
                uint32_t numCleanRegions,
                JobScheduler& scheduler,
                RegionEvictCallback evictCb,
                std::vector<uint32_t> sizeClasses,
                std::unique_ptr<EvictionPolicy> policy);
  RegionManager(const RegionManager&) = delete;
  RegionManager& operator=(const RegionManager&) = delete;

  Region& getRegion(RegionId rid) {
    XDCHECK(rid.valid());
    return *regions_[rid.index()];
  }

  const Region& getRegion(RegionId rid) const {
    XDCHECK(rid.valid());
    return *regions_[rid.index()];
  }

  const std::vector<uint32_t>& getSizeClasses() const { return sizeClasses_; }

  void pin(Region& region) {
    region.setPinned();
    pinnedCount_.inc();
  }

  uint64_t pinnedCount() const { return pinnedCount_.get(); }

  uint64_t regionSize() const { return regionSize_; }

  uint32_t blockSize() const { return blockSize_; }

  // Gets a free region if any left
  RegionId getFree();

  uint32_t numFree() const { return numFree_; }

  // Gets a region to evict
  RegionId evict();

  void touch(RegionId rid) { policy_->touch(rid); }

  // Calling track on tracked regions is noop
  void track(RegionId rid) { policy_->track(rid); }

  void reset();

  uint64_t getSeqNumber() const {
    return seqNumber_.load(std::memory_order_acquire);
  }

  AbsAddress toAbsolute(RelAddress ra) const {
    return AbsAddress{ra.offset() + ra.rid().index() * regionSize_};
  }

  RelAddress toRelative(AbsAddress aa) const {
    // Compiler optimizes to use one division instruction
    return RelAddress{RegionId(aa.offset() / regionSize_),
                      uint32_t(aa.offset() % regionSize_)};
  }

  Buffer makeIOBuffer(uint32_t size) const {
    return device_.makeIOBuffer(size);
  }

  bool write(RelAddress addr, BufferView buf) const;
  bool read(RelAddress addr, MutableBufferView buf) const;
  void flush() const;

  std::mutex& getLock(RegionId rid) const {
    return regionMutexes_[rid.index() % kNumLocks];
  }

  // Stores region information in a Thrift object for all regions
  void persist(RecordWriter& rw) const;

  // Resets RegionManager and recovers region data. Throws std::exception on
  // failure.
  void recover(RecordReader& rr);

  void getCounters(const CounterVisitor& visitor) const;

  const folly::F14FastMap<uint16_t, AtomicCounter> getRegionsByClassId() const {
    return regionsByClassId_;
  }

  uint32_t getRegionSlotSize(RegionId rid) const {
    const auto& region = getRegion(rid);
    if (sizeClasses_.empty() || region.isPinned()) {
      return 0;
    }
    return sizeClasses_[region.getClassId()];
  }

  RegionDescriptor openForRead(RegionId rid, uint64_t seqNumber);
  void close(RegionDescriptor&& desc);
  OpenStatus getCleanRegion(RegionId& rid);
  void scheduleReclaim();
  JobExitCode startReclaim();
  void releaseEvictedRegion(RegionId rid, std::chrono::nanoseconds startTime);
  void doEviction(RegionId rid, BufferView buffer) const;

 private:
  uint64_t physicalOffset(RelAddress addr) const {
    return baseOffset_ + toAbsolute(addr).offset();
  }

  bool isValidIORange(uint32_t offset, uint32_t size) const;

  // Detects @numFree_ and tracks appropriate regions. Called during recovery.
  void detectFree();

  static constexpr uint32_t kNumLocks = 256;

  const uint32_t numRegions_{};
  const uint64_t regionSize_{};
  const uint64_t baseOffset_{};
  const uint32_t blockSize_{};
  Device& device_;
  const std::unique_ptr<EvictionPolicy> policy_;
  std::unique_ptr<std::unique_ptr<Region>[]> regions_;
  uint32_t numFree_{};
  mutable folly::F14FastMap<uint16_t, AtomicCounter> regionsByClassId_;
  mutable AtomicCounter pinnedCount_;
  mutable AtomicCounter physicalWrittenCount_;
  mutable std::mutex regionMutexes_[kNumLocks];

  mutable std::mutex cleanRegionsMutex_;
  std::vector<RegionId> cleanRegions_;
  const uint32_t numCleanRegions_{};
  std::atomic<bool> outOfRegions_{false};

  std::atomic<uint64_t> seqNumber_{0};

  uint32_t reclaimsScheduled_{0};
  JobScheduler& scheduler_;

  const RegionEvictCallback evictCb_;

  const std::vector<uint32_t> sizeClasses_;
  //
  // To understand naming here, let me explain difference between "reclamation"
  // and "eviction". Cache evicts item and makes it inaccessible via lookup. It
  // is an item level operation. When we say "reclamation" about regions we
  // refer to wiping an entire region for reuse. As part of reclamation, every
  // item in the region gets evicted.
  mutable AtomicCounter reclaimCount_;
  mutable AtomicCounter reclaimTimeCountUs_;
  mutable AtomicCounter evictedCount_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

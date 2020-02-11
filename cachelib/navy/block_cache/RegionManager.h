#pragma once

#include <cassert>
#include <memory>
#include <mutex>
#include <utility>

#include <folly/container/F14Map.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/Region.h"
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

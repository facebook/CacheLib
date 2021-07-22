#pragma once

#include <folly/container/F14Map.h>

#include <cassert>
#include <memory>
#include <mutex>
#include <utility>

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
  // @param device            reference to device
  // @param numCleanRegions   How many regions reclamator maintains in
  //                          the clean pool
  // @param scheduler         JobScheduler to run reclamation jobs
  // @param regionEvictCb     Callback invoked when region evicted
  // @param sizeClasses       list of size classes
  // @param policy            eviction policy
  // @param numInMemBuffers   number of in memory buffers
  // @Param numPriorities     max number of priorities allowed for regions
  RegionManager(uint32_t numRegions,
                uint64_t regionSize,
                uint64_t baseOffset,
                Device& device,
                uint32_t numCleanRegions,
                JobScheduler& scheduler,
                RegionEvictCallback evictCb,
                std::vector<uint32_t> sizeClasses,
                std::unique_ptr<EvictionPolicy> policy,
                uint32_t numInMemBuffers,
                uint16_t numPriorities);
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

  void doFlush(RegionId rid, bool async);

  uint64_t regionSize() const { return regionSize_; }

  // Gets a region to evict
  RegionId evict();

  void touch(RegionId rid) { policy_->touch(rid); }

  // Calling track on tracked regions is noop
  void track(RegionId rid);

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

  // Assign a buffer from buffer pool
  std::unique_ptr<Buffer> claimBufferFromPool();

  // Return the buffer to the pool
  void returnBufferToPool(std::unique_ptr<Buffer> buf) {
    {
      std::lock_guard<std::mutex> bufLock{bufferMutex_};
      buffers_.push_back(std::move(buf));
    }
    numInMemBufActive_.dec();
  }

  // writes buffer @buf at the @addr
  // @addr must be the address returned by Region::open(OpenMode::Write)
  // @buf may be mutated and will be de-allocated at the end of this
  bool write(RelAddress addr, Buffer buf);

  // returns a buffer with data read from the device the @addr of size bytes
  // @addr must be the address returned by Region::open(OpenMode::Read)
  //
  // On success the returned buffer will have same size as "size" argument.
  // Caller must check the size of the buffer returned to determine if this
  // succeeded or not.
  Buffer read(const RegionDescriptor& desc, RelAddress addr, size_t size) const;

  // flushes all in memory buffers to the device and then issues device flush
  void flush();
  bool flushBuffer(const RegionId& rid);

  // Stores region information in a Thrift object for all regions
  void persist(RecordWriter& rw) const;

  // Resets RegionManager and recovers region data. Throws std::exception on
  // failure.
  void recover(RecordReader& rr);

  void getCounters(const CounterVisitor& visitor) const;

  uint32_t getRegionSlotSize(RegionId rid) const {
    const auto& region = getRegion(rid);
    if (sizeClasses_.empty()) {
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
  bool doesBufferingWrites() const { return numInMemBuffers_ > 0; }

 private:
  using LockGuard = std::lock_guard<std::mutex>;
  uint64_t physicalOffset(RelAddress addr) const {
    return baseOffset_ + toAbsolute(addr).offset();
  }

  bool deviceWrite(RelAddress addr, Buffer buf);

  bool isValidIORange(uint32_t offset, uint32_t size) const;
  OpenStatus assignBufferToRegion(RegionId rid);

  // Initialize the eviction policy. Even on a clean start, we will track all
  // the regions. The difference is that these regions will have no items in
  // them and can be evicted right away.
  void resetEvictionPolicy();

  const uint16_t numPriorities_{};
  const uint32_t numRegions_{};
  const uint64_t regionSize_{};
  const uint64_t baseOffset_{};
  Device& device_;
  const std::unique_ptr<EvictionPolicy> policy_;
  std::unique_ptr<std::unique_ptr<Region>[]> regions_;
  mutable AtomicCounter externalFragmentation_;

  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter reclaimRegionErrors_;

  mutable std::mutex cleanRegionsMutex_;
  std::vector<RegionId> cleanRegions_;
  const uint32_t numCleanRegions_{};

  std::atomic<uint64_t> seqNumber_{0};

  uint32_t reclaimsScheduled_{0};
  JobScheduler& scheduler_;

  const RegionEvictCallback evictCb_;

  const std::vector<uint32_t> sizeClasses_;

  // To understand naming here, let me explain difference between "reclamation"
  // and "eviction". Cache evicts item and makes it inaccessible via lookup. It
  // is an item level operation. When we say "reclamation" about regions we
  // refer to wiping an entire region for reuse. As part of reclamation, every
  // item in the region gets evicted.
  mutable AtomicCounter reclaimCount_;
  mutable AtomicCounter reclaimTimeCountUs_;
  mutable AtomicCounter evictedCount_;

  // Stats to keep track of inmem buffer usage
  mutable AtomicCounter numInMemBufActive_;
  mutable AtomicCounter numInMemBufWaitingFlush_;
  mutable AtomicCounter numInMemBufFlushRetires_;

  const uint32_t numInMemBuffers_{0};
  // Locking order is region lock, followed by bufferMutex_;
  mutable std::mutex bufferMutex_;
  std::vector<std::unique_ptr<Buffer>> buffers_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

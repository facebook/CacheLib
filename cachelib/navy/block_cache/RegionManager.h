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

#pragma once

#include <folly/Random.h>
#include <folly/container/F14Map.h>
#include <folly/fibers/TimedMutex.h>

#include <cassert>
#include <memory>
#include <utility>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/ConditionVariable.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/block_cache/Region.h"
#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/NavyThread.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
using folly::fibers::TimedMutex;
using CondWaiter = util::ConditionVariable::Waiter;

// Callback that is used to clear index.
//   @rid       Region ID
//   @buffer    Buffer with region data, valid during callback invocation
// Returns number of slots evicted
using RegionEvictCallback =
    std::function<uint32_t(RegionId rid, BufferView buffer)>;

// Callback that is used to clean up region.
//   @rid       Region ID
//   @buffer    Buffer with region data, valid during callback invocation
using RegionCleanupCallback =
    std::function<void(RegionId rid, BufferView buffer)>;

// Size class or stack allocator. Thread safe. Syncs access, reclaims regions
// Controls the allocation of regions, status (open for read/write), and
// eviction. Region manager doesn't have internal locks. External caller must
// take care of locking.
class RegionManager {
 public:
  // Constructs a Region Manager.
  //
  // @param numRegions                number of regions
  // @param regionSize                size of the region
  // @param baseOffset                base offset of the region
  // @param device                    reference to device
  // @param numCleanRegions           How many regions reclamator maintains in
  //                                  the clean pool
  // @param scheduler                 JobScheduler to run reclamation jobs
  // @param numWorkers                Number of threads to run reclamation jobs
  // @param stackSize                 Fiber stack size for each worker thread.
  //                                  0 for default
  // @param evictCb                   Callback invoked when region evicted
  // @param cleanupCb                 Callback invoked when region cleaned up
  // @param policy                    eviction policy
  // @param numInMemBuffers           number of in memory buffers
  // @param numPriorities             max number of priorities allowed for
  //                                  regions
  // @param inMemBufFlushRetryLimit   max number of flushing retry times for
  //                                  in-mem buffer
  RegionManager(uint32_t numRegions,
                uint64_t regionSize,
                uint64_t baseOffset,
                Device& device,
                uint32_t numCleanRegions,
                uint32_t numWorkers,
                uint32_t stackSize,
                RegionEvictCallback evictCb,
                RegionCleanupCallback cleanupCb,
                std::unique_ptr<EvictionPolicy> policy,
                uint32_t numInMemBuffers,
                uint16_t numPriorities,
                uint16_t inMemBufFlushRetryLimit);
  RegionManager(const RegionManager&) = delete;
  RegionManager& operator=(const RegionManager&) = delete;

  // Destroy the worker thread for safety first
  ~RegionManager() { workers_.clear(); }

  // return the size of usable space
  uint64_t getSize() const {
    return static_cast<uint64_t>(numRegions_) * regionSize_;
  }

  // Gets a region from a valid region ID.
  Region& getRegion(RegionId rid) {
    XDCHECK(rid.valid());
    return *regions_[rid.index()];
  }

  // Gets a const region from a valid region ID.
  const Region& getRegion(RegionId rid) const {
    XDCHECK(rid.valid());
    return *regions_[rid.index()];
  }

  const RegionId getRandomRegion() const {
    return RegionId{folly::Random::rand32(0, numRegions_)};
  }
  // Flushes the in memory buffer attached to a region in either async or
  // sync mode.
  // In async mode, a flush job will be added to a job scheduler;
  // In sync mode, the function will not end until the flush work succeeds.
  void doFlush(RegionId rid, bool async);

  // Returns the size of one region.
  uint64_t regionSize() const { return regionSize_; }

  // Gets a region to evict.
  RegionId evict();

  // Promote a region. If this region was still buffered in-mem,
  // this would be a no-op.
  void touch(RegionId rid);

  // Calling track on tracked regions is noop.
  void track(RegionId rid);

  // Resets all region internal state.
  void reset();

  // Atomically loads the current sequence number (in memory_order_acquire
  // order).
  // Sequence number increases when a reclamation finished. Since reclamation
  // may start during reading, by checking whether the sequence number changes,
  // we avoid reading a region that has been reclaimed.
  uint64_t getSeqNumber() const {
    return seqNumber_.load(std::memory_order_acquire);
  }

  // Converts @RelAddress to @AbsAddress.
  AbsAddress toAbsolute(RelAddress ra) const {
    return AbsAddress{ra.offset() + ra.rid().index() * regionSize_};
  }

  // Converts @AbsAddress to @RelAddress.
  RelAddress toRelative(AbsAddress aa) const {
    // Compiler optimizes to use one division instruction
    return RelAddress{RegionId(aa.offset() / regionSize_),
                      uint32_t(aa.offset() % regionSize_)};
  }

  // Assigns a buffer from buffer pool.
  std::pair<std::unique_ptr<Buffer>, std::unique_ptr<CondWaiter>>
  claimBufferFromPool(bool addWaiter);

  // Returns the buffer to the pool.
  void returnBufferToPool(std::unique_ptr<Buffer> buf) {
    {
      std::lock_guard<TimedMutex> bufLock{bufferMutex_};
      buffers_.push_back(std::move(buf));
      if (bufferCond_.numWaiters() > 0) {
        bufferCond_.notifyAll();
      }
    }
    numInMemBufActive_.dec();
  }

  // Writes buffer @buf at the @addr.
  // @addr must be the address returned by Region::open(OpenMode::Write)
  // @buf may be mutated and will be de-allocated at the end of this
  void write(RelAddress addr, Buffer buf);

  bool deviceWrite(RelAddress addr, Buffer buf);

  // Returns a buffer with data read from the device the @addr of size bytes
  // @addr must be the address returned by Region::open(OpenMode::Read).
  //
  // On success the returned buffer will have same size as "size" argument.
  // Caller must check the size of the buffer returned to determine if this
  // succeeded or not.
  Buffer read(const RegionDescriptor& desc, RelAddress addr, size_t size) const;

  // Flushes all in memory buffers to the device and then issues device flush.
  void flush();

  // Flushes the in memory buffer attached to a region.
  // Returns true if the flush succeeds and the buffer is detached from the
  // region; false otherwise.
  //
  // Caller is expected to call flushBuffer until true is returned or retry
  // times reach the limit. This routine is idempotent and is safe to call
  // multiple times until detachBuffer is done.
  Region::FlushRes flushBuffer(const RegionId& rid);

  // Detaches the buffer from the region and returns the buffer to pool.
  // This could block if there are active readers
  void detachBuffer(const RegionId& rid);

  // Cleans up the in memory buffer when flushing failure reach the retry limit.
  // This could block if there are active readers or writers
  void cleanupBufferOnFlushFailure(const RegionId& rid);

  // Releases a region that was cleaned up due to in-mem buffer flushing
  // failure.
  void releaseCleanedupRegion(RegionId rid);

  // Stores region information in a Thrift object for all regions.
  void persist(RecordWriter& rw) const;

  // Resets RegionManager and recovers region data. Throws std::exception on
  // failure.
  void recover(RecordReader& rr);

  // Exports RegionManager stats via CounterVisitor.
  void getCounters(const CounterVisitor& visitor) const;

  // Opens a region for reading and returns the region descriptor.
  //
  // @param rid         region ID
  // @param seqNumber   the sequence number aqcuired before opening the region
  //                    for read; it is used to determine whether a reclamation
  //                    happened during reading
  RegionDescriptor openForRead(RegionId rid, uint64_t seqNumber);

  // Closes the region and consumes the region descriptor.
  void close(RegionDescriptor&& desc);

  // Fetches a clean region from the @cleanRegions_ list and schedules reclaim
  // jobs to refill the list. If in-mem buffer mode is enabled, a buffer will be
  // attached to the fetched clean region.
  // Returns OpenStatus::Ready if all the operations are successful;
  // OpenStatus::Retry otherwise.
  std::pair<OpenStatus, std::unique_ptr<CondWaiter>> getCleanRegion(
      RegionId& rid, bool addWaiter);

  // Finish all pending jobs
  void drain();

  // Schedules region reclaim job to create a clean region
  void startReclaim();

  // Releases a region that was evicted during region reclamation.
  //
  // @param rid        region ID
  // @param startTime  time when a reclamation starts;
  //                   it is used to count the reclamation time duration
  void releaseEvictedRegion(RegionId rid, std::chrono::nanoseconds startTime);

  // Evicts a region by calling @evictCb_ during region reclamation.
  void doEviction(RegionId rid, BufferView buffer) const;

 private:
  using LockGuard = std::lock_guard<TimedMutex>;
  uint64_t physicalOffset(RelAddress addr) const {
    return baseOffset_ + toAbsolute(addr).offset();
  }

  NavyThread& getNextWorker() {
    return *(workers_[numReclaimScheduled_.add_fetch(1) % workers_.size()]);
  }

  void doReclaim();
  void doFlushInternal(RegionId rid);

  bool deviceWrite(RelAddress addr, BufferView buf);

  bool isValidIORange(uint32_t offset, uint32_t size) const;
  std::pair<OpenStatus, std::unique_ptr<CondWaiter>> assignBufferToRegion(
      RegionId rid, bool addWaiter);

  // Initializes the eviction policy. Even on a clean start, we will track all
  // the regions. The difference is that these regions will have no items in
  // them and can be evicted right away.
  void resetEvictionPolicy();

  const uint16_t numPriorities_{};
  const uint16_t inMemBufFlushRetryLimit_{};
  const uint32_t numRegions_{};
  const uint64_t regionSize_{};
  const uint64_t baseOffset_{};
  Device& device_;
  const std::unique_ptr<EvictionPolicy> policy_;
  mutable util::ConditionVariable evictCond_;
  std::unique_ptr<std::unique_ptr<Region>[]> regions_;
  mutable AtomicCounter externalFragmentation_;

  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter reclaimRegionErrors_;

  mutable TimedMutex cleanRegionsMutex_;
  mutable util::ConditionVariable cleanRegionsCond_;
  std::vector<RegionId> cleanRegions_;
  const uint32_t numCleanRegions_{};
  mutable AtomicCounter cleanRegionRetries_;

  std::atomic<uint64_t> seqNumber_{0};

  uint32_t reclaimsOutstanding_{0};

  // The thread that runs the flush and reclaim. For Navy-async thread mode, the
  // async flushes will be run in-line on fiber by the async NavyThread itself
  std::vector<std::unique_ptr<NavyThread>> workers_;
  mutable AtomicCounter numReclaimScheduled_;

  const RegionEvictCallback evictCb_;
  const RegionCleanupCallback cleanupCb_;

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
  mutable AtomicCounter numInMemBufFlushRetries_;
  mutable AtomicCounter numInMemBufFlushFailures_;

  const uint32_t numInMemBuffers_{0};
  // Locking order is region lock, followed by bufferMutex_;
  mutable TimedMutex bufferMutex_;
  mutable util::ConditionVariable bufferCond_;
  std::vector<std::unique_ptr<Buffer>> buffers_;
  int placementHandle_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

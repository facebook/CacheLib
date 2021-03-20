#pragma once

#include <folly/SharedMutex.h>

#include <condition_variable>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/kangaroo/ChainedLogIndex.h"
#include "cachelib/navy/kangaroo/KangarooLogSegment.h"

namespace facebook {
namespace cachelib {
namespace navy {
class KangarooLog {
 public:
  struct Config {
    uint32_t readSize{4 * 1024};
    uint32_t segmentSize{256 * 1024};

    // The range of device that Log will access is guaranted to be
    // with in [logBaseOffset, logBaseOffset + logSize)
    uint64_t logBaseOffset{};
    uint64_t logSize{0};
    Device* device{nullptr};

    // log partitioning
    uint64_t logPhysicalPartitions{};

    // for index
    uint64_t logIndexPartitions{};
    uint16_t sizeAllocations{1024};
    uint64_t numTotalIndexBuckets{};
    SetNumberCallback setNumberCallback{};

    // for merging to sets
    uint32_t threshold;
    SetMultiInsertCallback setMultiInsertCallback{};
    uint64_t mergeThreads{32};

    Config& validate();
  };

  // Throw std::invalid_argument on bad config
  explicit KangarooLog(Config&& config);

  ~KangarooLog();

  KangarooLog(const KangarooLog&) = delete;
  KangarooLog& operator=(const KangarooLog&) = delete;

  bool couldExist(HashedKey hk);

  // Look up a key in KangarooLog. On success, it will return Status::Ok and
  // populate "value" with the value found. User should pass in a null
  // Buffer as "value" as any existing storage will be freed. If not found,
  // it will return Status::NotFound. And of course, on error, it returns
  // DeviceError.
  Status lookup(HashedKey hk, Buffer& value);

  // Inserts key and value into KangarooLog. This will replace an existing
  // key if found. If it failed to write, it will return DeviceError.
  Status insert(HashedKey hk, BufferView value);

  // Removes an entry from Kangaroo if found. Ok on success, NotFound on miss,
  // and DeviceError on error.
  Status remove(HashedKey hk);

  void flush();

  void reset();

  double falsePositivePct() const;
  double extraReadsPct() const;
  double fragmentationPct() const;
  uint64_t getBytesWritten() const;

  // TODO: persist and recover not implemented

 private:
  struct ValidConfigTag {};
  KangarooLog(Config&& config, ValidConfigTag);

  Buffer readLogPage(LogPageId lpid);
  Buffer readLogSegment(LogSegmentId lsid);
  bool writeLogSegment(LogSegmentId lsid, Buffer buffer);
  bool flushLogSegment(LogSegmentId lsid);

  bool isBuffered(LogPageId lpid,
                  uint64_t physicalPartition); // does not grab logSegmentMutex
                                               // mutex
  Status lookupBuffered(HashedKey hk, Buffer& value, LogPageId lpid);
  Status lookupBufferedTag(uint32_t tag,
                           HashedKey& hk,
                           Buffer& value,
                           LogPageId lpid);

  uint64_t getPhysicalPartition(LogPageId lpid) const {
    return lpid.index() / pagesPerPartition_;
  }
  uint64_t getLogSegmentOffset(LogSegmentId lsid) const {
    return logBaseOffset_ + segmentSize_ * lsid.index() +
           physicalPartitionSize_ * lsid.partition();
  }

  uint64_t getPhysicalPartition(HashedKey hk) const {
    return getIndexPartition(hk) % logPhysicalPartitions_;
  }
  uint64_t getIndexPartition(HashedKey hk) const {
    return getLogIndexEntry(hk) % logIndexPartitions_;
  }
  uint64_t getLogIndexEntry(HashedKey hk) const {
    return setNumberCallback_(hk.keyHash()).index() % numIndexEntries_;
  }

  uint64_t getLogPageOffset(LogPageId lpid) const {
    return logBaseOffset_ + pageSize_ * lpid.index();
  }

  LogPageId getLogPageId(PartitionOffset po, uint32_t physicalPartition) {
    return LogPageId(po.index() + physicalPartition * pagesPerPartition_,
                     po.isValid());
  }
  PartitionOffset getPartitionOffset(LogPageId lpid) {
    return PartitionOffset(lpid.index() % pagesPerPartition_, lpid.isValid());
  }

  LogSegmentId getSegmentId(LogPageId lpid) const {
    uint32_t index = (lpid.index() % pagesPerPartition_) / pagesPerSegment_;
    return LogSegmentId(index, getPhysicalPartition(lpid));
  }

  LogPageId getPageId(LogSegmentId lsid) const {
    uint64_t i =
        lsid.partition() * pagesPerPartition_ + lsid.index() * pagesPerSegment_;
    return LogPageId(i, true);
  }

  LogSegmentId getNextLsid(LogSegmentId lsid);

  // locks based on partition number, concurrent read, single modify
  folly::SharedMutex& getMutexFromSegment(LogSegmentId lsid) const {
    return mutex_[(lsid.partition()) & (NumMutexes - 1)];
  }
  folly::SharedMutex& getMutexFromPage(LogPageId lpid) const {
    return getMutexFromSegment(getSegmentId(lpid));
  }

  double cleaningThreshold_ = .1;
  bool shouldClean(uint64_t nextWriteLog, uint64_t nextCleaningLoc);
  void cleanSegment(LogSegmentId lsid);
  void cleanSegmentsLoop(uint64_t threadId);
  bool shouldWakeCompaction(uint64_t threadId);
  void moveBucket(HashedKey hk, uint64_t count, LogSegmentId lsidToFlush);
  void readmit(HashedKey hk, BufferView value);

  // Use birthday paradox to estimate number of mutexes given number of parallel
  // queries and desired probability of lock collision.
  static constexpr size_t NumMutexes = 16 * 1024;

  // Serialization format version. Never 0. Versions < 10 reserved for testing.
  static constexpr uint32_t kFormatVersion = 10;

  const uint64_t pageSize_{};
  const uint64_t segmentSize_{};
  const uint64_t logBaseOffset_{};
  const uint64_t logSize_{};
  const uint64_t pagesPerSegment_{};

  Device& device_;
  std::unique_ptr<folly::SharedMutex[]> mutex_{
      new folly::SharedMutex[NumMutexes]};
  const uint64_t logIndexPartitions_{};
  ChainedLogIndex** index_;

  const uint64_t logPhysicalPartitions_{};
  const uint64_t physicalPartitionSize_{};
  const uint64_t pagesPerPartition_{};
  const uint64_t numIndexEntries_{};
  const uint64_t segmentsPerPartition_{};
  KangarooLogSegment** currentLogSegments_;
  /* prevent access to log segment while it's being switched out
   * to disk, one for each physical partition */
  std::unique_ptr<folly::SharedMutex[]> logSegmentMutexs_;
  Buffer* logSegmentBuffers_;

  // background thread to read log segments and write them
  // to sets
  std::vector<std::thread> logToSetsThreads_;
  uint64_t numThreads_;
  std::mutex writeSetsMutex_;
  std::condition_variable writeSetsCv_;
  std::condition_variable_any flushLogCv_;
  bool killThread_{false};
  std::unique_ptr<LogSegmentId[]> nextLsidsToClean_;
  std::unique_ptr<uint64_t[]> nextCleaningPartition_;
  const SetNumberCallback setNumberCallback_{};
  SetMultiInsertCallback setMultiInsertCb_;
  uint32_t threshold_{0};

  mutable AtomicCounter itemCount_;
  mutable AtomicCounter insertCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter evictionCount_;
  mutable AtomicCounter keyCollisionCount_;
  mutable AtomicCounter logicalWrittenCount_;
  mutable AtomicCounter physicalWrittenCount_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter checksumErrorCount_;
  mutable AtomicCounter flushPageReads_;
  mutable AtomicCounter flushFalsePageReads_;
  mutable AtomicCounter flushLogSegmentsCount_;
  mutable AtomicCounter moveBucketCalls_;
  mutable AtomicCounter notFoundInLogIndex_;
  mutable AtomicCounter foundInLogIndex_;
  mutable AtomicCounter indexSegmentMismatch_;
  mutable AtomicCounter replaceIndexInsert_;
  mutable AtomicCounter indexReplacementReinsertions_;
  mutable AtomicCounter indexReinsertions_;
  mutable AtomicCounter indexReinsertionFailed_;
  mutable AtomicCounter moveBucketSuccessfulRets_;
  mutable AtomicCounter thresholdNotHit_;
  mutable AtomicCounter sumCountCounter_;
  mutable AtomicCounter numCountCalls_;
  mutable AtomicCounter readmitBytes_;
  mutable AtomicCounter readmitRequests_;
  mutable AtomicCounter readmitRequestsFailed_;
  mutable AtomicCounter logSegmentsWrittenCount_;
  mutable AtomicCounter bytesInserted_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

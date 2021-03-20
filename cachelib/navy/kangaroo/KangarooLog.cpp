#include "cachelib/navy/kangaroo/KangarooLog.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <chrono>
#include <mutex>
#include <shared_mutex>

namespace facebook {
namespace cachelib {
namespace navy {

Buffer KangarooLog::readLogPage(LogPageId lpid) {
  auto buffer = device_.makeIOBuffer(pageSize_);
  XDCHECK(!buffer.isNull());

  const bool res =
      device_.read(getLogPageOffset(lpid), buffer.size(), buffer.data());
  if (!res) {
    return {};
  }
  // TODO: checksumming & generations
  return buffer;
}

Buffer KangarooLog::readLogSegment(LogSegmentId lsid) {
  auto buffer = device_.makeIOBuffer(segmentSize_);
  XDCHECK(!buffer.isNull());

  const bool res =
      device_.read(getLogSegmentOffset(lsid), buffer.size(), buffer.data());

  if (!res) {
    return {};
  }
  // TODO: checksumming & generations
  return buffer;
}

bool KangarooLog::writeLogSegment(LogSegmentId lsid, Buffer buffer) {
  // TODO: set checksums
  logSegmentsWrittenCount_.inc();
  return device_.write(getLogSegmentOffset(lsid), std::move(buffer));
}

bool KangarooLog::flushLogSegment(LogSegmentId lsid) {
  LogSegmentId nextLsid = getNextLsid(lsid);
  {
    std::unique_lock<folly::SharedMutex> segmentLock{getMutexFromSegment(lsid)};
    while (nextLsid == nextLsidsToClean_[lsid.partition()] && !killThread_) {
      writeSetsCv_.notify_all();
      flushLogCv_.wait_for(segmentLock, std::chrono::seconds(30));
    }
    if (killThread_) {
      return false;
    }
    std::unique_lock<folly::SharedMutex> bufferLock{
        logSegmentMutexs_[lsid.partition()]};

    if (currentLogSegments_[lsid.partition()]->getLogSegmentId() == lsid) {
      // another thread already flushed log
      writeLogSegment(
          lsid, Buffer(logSegmentBuffers_[lsid.partition()].view(), pageSize_));
      currentLogSegments_[lsid.partition()]->clear(nextLsid);
    }
  }
  return true;
}

LogSegmentId KangarooLog::getNextLsid(LogSegmentId lsid) {
  return LogSegmentId(
      (lsid.index() + 1) % (pagesPerPartition_ / pagesPerSegment_),
      lsid.partition());
}

KangarooLog::~KangarooLog() {
  killThread_ = true;
  for (uint64_t i = 0; i < numThreads_; i++) {
    writeSetsCv_.notify_all();
    flushLogCv_.notify_all();
    logToSetsThreads_[i].join();
  }
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    delete index_[i];
  }
  delete[] index_;
  for (uint64_t i = 0; i < logPhysicalPartitions_; i++) {
    delete currentLogSegments_[i];
  }
}

KangarooLog::KangarooLog(Config&& config)
    : KangarooLog{std::move(config.validate()), ValidConfigTag{}} {}

KangarooLog::Config& KangarooLog::Config::validate() {
  if (logSize < readSize) {
    throw std::invalid_argument(
        folly::sformat("log size: {} cannot be smaller than read size: {}",
                       logSize,
                       readSize));
  }

  if (logSize < segmentSize) {
    throw std::invalid_argument(
        folly::sformat("log size: {} cannot be smaller than segment size: {}",
                       logSize,
                       readSize));
  }

  if (!folly::isPowTwo(readSize)) {
    throw std::invalid_argument(
        folly::sformat("invalid read size: {}", readSize));
  }

  if (logSize > uint64_t{readSize} << 32) {
    throw std::invalid_argument(folly::sformat(
        "Can't address kangaroo log with 32 bits. Log size: {}, read size: {}",
        logSize,
        readSize));
  }

  if (segmentSize % readSize != 0 || logSize % readSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "logSize and segmentSize need to be a multiple of readSize. "
        "segmentSize: {}, logSize:{}, readSize: {}.",
        segmentSize,
        logSize,
        readSize));
  }

  if (logSize % segmentSize != 0) {
    throw std::invalid_argument(
        folly::sformat("logSize must be a multiple of segmentSize. "
                       "logSize:{}, segmentSize: {}.",
                       logSize,
                       segmentSize));
  }

  if (logPhysicalPartitions == 0) {
    throw std::invalid_argument(folly::sformat(
        "number physical partitions needs to be greater than 0"));
  }

  if (logIndexPartitions % logPhysicalPartitions != 0) {
    throw std::invalid_argument(
        folly::sformat("the number of index partitions must be a multiple of "
                       "the physical partitions"));
  }

  if (logSize / logPhysicalPartitions % readSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "Phycial partition size must be a multiple of read size"));
  }

  if (numTotalIndexBuckets % logIndexPartitions != 0) {
    throw std::invalid_argument(folly::sformat(
        "Index entries {} must be a multiple of index partitions {}",
        numTotalIndexBuckets, logIndexPartitions));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  if (numTotalIndexBuckets == 0) {
    throw std::invalid_argument("need to have a number of index buckets");
  }

  return *this;
}

KangarooLog::KangarooLog(Config&& config, ValidConfigTag)
    : pageSize_{config.readSize},
      segmentSize_{config.segmentSize},
      logBaseOffset_{config.logBaseOffset},
      logSize_{config.logSize},
      pagesPerSegment_{segmentSize_ / pageSize_},
      device_{*config.device},
      logIndexPartitions_{config.logIndexPartitions},
      index_{new ChainedLogIndex*[logIndexPartitions_]},
      logPhysicalPartitions_{config.logPhysicalPartitions},
      physicalPartitionSize_{logSize_ / logPhysicalPartitions_},
      pagesPerPartition_{physicalPartitionSize_ / pageSize_},
      numIndexEntries_{config.numTotalIndexBuckets},
      segmentsPerPartition_{pagesPerPartition_ / pagesPerSegment_},
      currentLogSegments_{new KangarooLogSegment*[logPhysicalPartitions_]},
      logSegmentBuffers_{new Buffer[logPhysicalPartitions_]},
      numThreads_{config.mergeThreads},
      nextLsidsToClean_{
          std::make_unique<LogSegmentId[]>(logPhysicalPartitions_)},
      nextCleaningPartition_{std::make_unique<uint64_t[]>(numThreads_)},
      setNumberCallback_{config.setNumberCallback},
      setMultiInsertCb_{config.setMultiInsertCallback},
      threshold_{config.threshold} {
  XLOGF(INFO,
        "Kangaroo Log created: size: {}, read size: {}, segment size: {}, base "
        "offset: {}, pages per partition {}",
        logSize_,
        pageSize_,
        segmentSize_,
        logBaseOffset_,
        pagesPerPartition_);
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    index_[i] = new ChainedLogIndex(numIndexEntries_ / logIndexPartitions_,
                                    config.sizeAllocations, setNumberCallback_);
  }
  logSegmentMutexs_ =
      std::make_unique<folly::SharedMutex[]>(logPhysicalPartitions_);
  reset();
  logToSetsThreads_.reserve(numThreads_);
  for (uint64_t i = 0; i < numThreads_; i++) {
    logToSetsThreads_.push_back(
        std::thread(&KangarooLog::cleanSegmentsLoop, this, i));
  }
}

bool KangarooLog::shouldClean(uint64_t nextWriteLoc, uint64_t nextCleaningLoc) {
  uint64_t freeSegments = 0;
  if (nextCleaningLoc >= nextWriteLoc) {
    freeSegments = nextCleaningLoc - nextWriteLoc;
  } else {
    freeSegments = nextCleaningLoc + (segmentsPerPartition_ - nextWriteLoc);
  }
  return freeSegments <= (segmentsPerPartition_ * cleaningThreshold_);
}

bool KangarooLog::shouldWakeCompaction(uint64_t threadId) {
  for (uint64_t i = 0; i < logPhysicalPartitions_; i++) {
    uint64_t partition =
        (i + nextCleaningPartition_[threadId]) % logPhysicalPartitions_;
    if (partition % numThreads_ == threadId) {
      std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[partition]};
      LogSegmentId currentLsid =
          currentLogSegments_[partition]->getLogSegmentId();
      // may want to stay ahead by more than 1 Lsid but should work for now
      if (shouldClean(getNextLsid(currentLsid).index(),
                      nextLsidsToClean_[partition].index())) {
        nextCleaningPartition_[threadId] = partition;
        return true;
      }
    }
  }
  // return true to kill thread if object is destructed
  return killThread_;
}

void KangarooLog::moveBucket(HashedKey hk,
                             uint64_t count,
                             LogSegmentId lsidToFlush) {
  KangarooBucketId bid = setNumberCallback_(hk.keyHash());
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  ChainedLogIndex::BucketIterator indexIt;

  std::vector<std::unique_ptr<ObjectInfo>> objects;
  objects.reserve(count);

  /* allow reinsertion to index if not enough objects to move */
  indexIt = index_[indexPartition]->getHashBucketIterator(hk);
  while (!indexIt.done()) {
    BufferView value;
    HashedKey key = hk;
    uint8_t hits;
    LogPageId lpid;
    uint32_t tag;

    if (killThread_) {
      return;
    }

    hits = static_cast<uint8_t>(indexIt.hits());
    tag = indexIt.tag();
    indexIt = index_[indexPartition]->getNext(indexIt);
    lpid =
        getLogPageId(index_[indexPartition]->find(bid, tag), physicalPartition);
    if (!lpid.isValid()) {
      continue;
    }

    // Find value, could be in in-memory buffer or on nvm
    Buffer buffer;
    Status status = lookupBufferedTag(tag, key, buffer, lpid);
    if (status != Status::Ok) {
      buffer = readLogPage(lpid);
      if (buffer.isNull()) {
        ioErrorCount_.inc();
        continue;
      }
      LogBucket* page = reinterpret_cast<LogBucket*>(buffer.data());
      flushPageReads_.inc();
      value = page->findTag(tag, key);
    } else {
      value = buffer.view();
    }

    if (value.isNull()) {
      index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
      continue;
    } else if (setNumberCallback_(key.keyHash()) != bid) {
      flushFalsePageReads_.inc();
      continue;
    }
    index_[indexPartition]->remove(tag, bid, getPartitionOffset(lpid));
    moveBucketSuccessfulRets_.inc();
    auto ptr = std::make_unique<ObjectInfo>(key, value, hits, lpid, tag);
    objects.push_back(std::move(ptr));
  }

  ReadmitCallback readmitCb = [&](std::unique_ptr<ObjectInfo>& oi) {
    /* reinsert items attempted to be moved into index unless in segment to
     * flush */
    moveBucketSuccessfulRets_.inc();
    if (getSegmentId(oi->lpid) != lsidToFlush) {
      index_[indexPartition]->insert(oi->tag, bid, getPartitionOffset(oi->lpid),
                                     oi->hits);
    } else if (oi->hits) {
      readmit(oi->key, oi->value.view());
    }
    return;
  };

  if (objects.size() < threshold_) {
    thresholdNotHit_.inc();
    for (auto& item : objects) {
      readmitCb(item);
    }
  } else {
    moveBucketCalls_.inc();
    setMultiInsertCb_(objects, readmitCb);
  }
}

void KangarooLog::cleanSegment(LogSegmentId lsid) {
  {
    std::shared_lock<folly::SharedMutex> segmentLock{getMutexFromSegment(lsid)};
    auto buf = readLogSegment(lsid);
    if (buf.isNull()) {
      ioErrorCount_.inc();
      return;
    }

    auto log_seg =
        KangarooLogSegment(segmentSize_, pageSize_, lsid, pagesPerPartition_,
                           buf.mutableView(), false);
    auto it = log_seg.getFirst();
    while (!it.done()) {
      uint64_t indexPartition = getIndexPartition(HashedKey(it.key()));
      uint32_t hits = 0;
      LogPageId lpid;
      lpid =
          getLogPageId(index_[indexPartition]->lookup(it.key(), false, &hits),
                       lsid.partition());
      if (!lpid.isValid() || lsid != getSegmentId(lpid)) {
        if (lpid.isValid()) {
          indexSegmentMismatch_.inc();
        }
        it = log_seg.getNext(it);
        notFoundInLogIndex_.inc();
        continue;
      }
      foundInLogIndex_.inc();

      // best effort count (remove could come and decrease
      // but that should be rare)
      uint64_t count;
      count = index_[indexPartition]->countBucket(it.key());
      sumCountCounter_.add(count);
      numCountCalls_.inc();
      if (count < threshold_) {
        thresholdNotHit_.inc();
        // evict key because set doesn't meet threshold
        if (hits) {
          readmit(it.key(), it.value());
        } else {
          index_[indexPartition]->remove(it.key(), getPartitionOffset(lpid));
        }
      } else {
        moveBucket(it.key(), count, lsid);
      }
      it = log_seg.getNext(it);
    }
  }
}

void KangarooLog::cleanSegmentsLoop(uint64_t threadId) {
  while (true) {
    while (!shouldWakeCompaction(threadId)) {
      flushLogCv_.notify_all();
      std::unique_lock<std::mutex> lock{writeSetsMutex_};
      writeSetsCv_.wait_for(lock, std::chrono::seconds(10));
    }
    writeSetsCv_.notify_all();
    if (killThread_) {
      return;
    }
    cleanSegment(nextLsidsToClean_[nextCleaningPartition_[threadId]]);
    nextLsidsToClean_[nextCleaningPartition_[threadId]] =
        getNextLsid(nextLsidsToClean_[nextCleaningPartition_[threadId]]);
    nextCleaningPartition_[threadId]++;
    flushLogSegmentsCount_.inc();

    // wake up any insert requests waiting for space
    flushLogCv_.notify_all();
  }
}

double KangarooLog::falsePositivePct() const {
  return 100. * keyCollisionCount_.get() /
         (lookupCount_.get() + removeCount_.get());
}

double KangarooLog::extraReadsPct() const {
  return 100. * flushFalsePageReads_.get() / flushPageReads_.get();
}

double KangarooLog::fragmentationPct() const {
  auto found = foundInLogIndex_.get();
  return 100. * found / (notFoundInLogIndex_.get() + found);
}

uint64_t KangarooLog::getBytesWritten() const {
  return logSegmentsWrittenCount_.get() * segmentSize_;
}

Status KangarooLog::lookup(HashedKey hk, Buffer& value) {
  lookupCount_.inc();
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  LogPageId lpid = getLogPageId(
      index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
  if (!lpid.isValid()) {
    return Status::NotFound;
  }

  Buffer buffer;
  BufferView valueView;
  LogBucket* page;
  {
    std::shared_lock<folly::SharedMutex> lock{getMutexFromPage(lpid)};

    // check if page is buffered in memory and read it
    Status ret = lookupBuffered(hk, value, lpid);
    if (ret != Status::Retry) {
      return ret;
    }

    buffer = readLogPage(lpid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
  }

  page = reinterpret_cast<LogBucket*>(buffer.data());

  valueView = page->find(hk);
  if (valueView.isNull()) {
    keyCollisionCount_.inc();
    return Status::NotFound;
  }

  value = Buffer{valueView};
  succLookupCount_.inc();
  return Status::Ok;
}

bool KangarooLog::couldExist(HashedKey hk) {
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  LogPageId lpid = getLogPageId(
      index_[indexPartition]->lookup(hk, true, nullptr), physicalPartition);
  if (!lpid.isValid()) {
    lookupCount_.inc();
    return false;
  }

  return true;
}

Status KangarooLog::insert(HashedKey hk, BufferView value) {
  LogPageId lpid;
  LogSegmentId lsid;
  uint64_t physicalPartition = getPhysicalPartition(hk);
  {
    // logSegment handles concurrent inserts
    // lock to prevent write out of segment
    std::shared_lock<folly::SharedMutex> lock{
        logSegmentMutexs_[physicalPartition]};
    lpid = currentLogSegments_[physicalPartition]->insert(hk, value);
    if (!lpid.isValid()) {
      // need to flush segment using lsid
      lsid = currentLogSegments_[physicalPartition]->getLogSegmentId();
    }
  }

  if (lpid.isValid()) {
    uint64_t indexPartition = getIndexPartition(hk);
    auto ret = index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
    if (ret == Status::NotFound) {
      replaceIndexInsert_.inc();
      ret = Status::Ok;
    }
    insertCount_.inc();
    if (ret == Status::Ok) {
      succInsertCount_.inc();
      bytesInserted_.add(hk.key().size() + value.size());
    }
    return ret;
  }

  if (flushLogSegment(lsid)) {
    flushLogCv_.notify_all();
    writeSetsCv_.notify_all();
    return insert(hk, value);
  } else {
    flushLogCv_.notify_all();
    writeSetsCv_.notify_all();
    return Status::Rejected;
  }
}

void KangarooLog::readmit(HashedKey hk, BufferView value) {
  LogPageId lpid;
  LogSegmentId lsid;
  uint64_t physicalPartition = getPhysicalPartition(hk);
  readmitRequests_.inc();

  {
    // logSegment handles concurrent inserts
    // lock to prevent write out of segment
    std::shared_lock<folly::SharedMutex> lock{
        logSegmentMutexs_[physicalPartition]};
    lpid = currentLogSegments_[physicalPartition]->insert(hk, value);
    if (!lpid.isValid()) {
      // no room in segment so will not insert
      readmitRequestsFailed_.inc();
      return;
    }
  }

  uint64_t indexPartition = getIndexPartition(hk);
  index_[indexPartition]->insert(hk, getPartitionOffset(lpid));
  readmitBytes_.add(hk.key().size() + value.size());
}

Status KangarooLog::remove(HashedKey hk) {
  uint64_t indexPartition = getIndexPartition(hk);
  uint64_t physicalPartition = getPhysicalPartition(hk);
  removeCount_.inc();
  LogPageId lpid;

  lpid = getLogPageId(index_[indexPartition]->lookup(hk, false, nullptr),
                      physicalPartition);
  if (!lpid.isValid()) {
    return Status::NotFound;
  }

  Buffer buffer;
  BufferView valueView;
  LogBucket* page;
  {
    std::shared_lock<folly::SharedMutex> lock{getMutexFromPage(lpid)};

    // check if page is buffered in memory and read it
    Status ret = lookupBuffered(hk, buffer, lpid);
    if (ret == Status::Ok) {
      Status status =
          index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
      if (status == Status::Ok) {
        succRemoveCount_.inc();
      }
      return status;
    } else if (ret == Status::Retry) {
      buffer = readLogPage(lpid);
      if (buffer.isNull()) {
        ioErrorCount_.inc();
        return Status::DeviceError;
      }
    } else {
      return ret;
    }
  }

  page = reinterpret_cast<LogBucket*>(buffer.data());

  valueView = page->find(hk);
  if (valueView.isNull()) {
    keyCollisionCount_.inc();
    return Status::NotFound;
  }

  Status status = index_[indexPartition]->remove(hk, getPartitionOffset(lpid));
  if (status == Status::Ok) {
    succRemoveCount_.inc();
  }
  return status;
}

void KangarooLog::flush() {
  // TODO: should probably flush buffered part of log
  return;
}

void KangarooLog::reset() {
  itemCount_.set(0);
  insertCount_.set(0);
  succInsertCount_.set(0);
  lookupCount_.set(0);
  succLookupCount_.set(0);
  removeCount_.set(0);
  logicalWrittenCount_.set(0);
  physicalWrittenCount_.set(0);
  ioErrorCount_.set(0);
  checksumErrorCount_.set(0);

  for (uint64_t i = 0; i < logPhysicalPartitions_; i++) {
    logSegmentBuffers_[i] = device_.makeIOBuffer(segmentSize_);
    currentLogSegments_[i] = new KangarooLogSegment(
        segmentSize_, pageSize_, LogSegmentId(0, i), pagesPerPartition_,
        logSegmentBuffers_[i].mutableView(), true);
    nextLsidsToClean_[i] = LogSegmentId(0, i);
  }
}

Status KangarooLog::lookupBuffered(HashedKey hk,
                                   Buffer& value,
                                   LogPageId lpid) {
  uint64_t partition = getPhysicalPartition(hk);
  BufferView view;
  {
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[partition]};
    if (!isBuffered(lpid, partition)) {
      return Status::Retry;
    }
    view = currentLogSegments_[partition]->find(hk, lpid);
    if (view.isNull()) {
      keyCollisionCount_.inc();
      return Status::NotFound;
    }
    value = Buffer{view};
  }
  return Status::Ok;
}

Status KangarooLog::lookupBufferedTag(uint32_t tag,
                                      HashedKey& hk,
                                      Buffer& value,
                                      LogPageId lpid) {
  uint64_t partition = getPhysicalPartition(lpid);
  BufferView view;
  {
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[partition]};
    if (!isBuffered(lpid, partition)) {
      return Status::Retry;
    }
    view = currentLogSegments_[partition]->findTag(tag, hk, lpid);
    if (view.isNull()) {
      keyCollisionCount_.inc();
      return Status::NotFound;
    }
    value = Buffer{view};
  }
  return Status::Ok;
}

bool KangarooLog::isBuffered(LogPageId lpid, uint64_t partition) {
  return getSegmentId(lpid) ==
         currentLogSegments_[partition]->getLogSegmentId();
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

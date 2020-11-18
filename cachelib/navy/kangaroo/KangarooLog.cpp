#include <mutex>
#include <shared_mutex>

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/kangaroo/KangarooLog.h"

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

bool KangarooLog::writeLogSegment(LogSegmentId lsid, MutableBufferView mutableView) {
  // TODO: set checksums 
  return device_.write(getLogSegmentOffset(lsid), 
      mutableView.size(), mutableView.data());
}

bool KangarooLog::flushLogSegment(LogSegmentId lsid) {
  LogSegmentId nextLsid = getNextLsid(lsid);
  {
    std::unique_lock<folly::SharedMutex> segmentLock{getMutexFromSegment(lsid)};
    flushLogCv_.wait(segmentLock, [&]{ return nextLsid != nextLsidsToClean_[lsid.partition()] || killThread_; } );
    if (killThread_) {
      return false;
    }
    std::unique_lock<folly::SharedMutex> bufferLock{logSegmentMutexs_[lsid.partition()]};

    if (currentLogSegments_[lsid.partition()]->getLogSegmentId() != lsid) {
      // another thread already flushed log
      return true;
    }

    writeLogSegment(lsid, logSegmentBuffers_[lsid.partition()].mutableView());
    currentLogSegments_[lsid.partition()]->clear(nextLsid);
  }
  flushLogCv_.notify_all();
  writeSetsCv_.notify_all();
  return true;
}

LogSegmentId KangarooLog::getNextLsid(LogSegmentId lsid) {
  return LogSegmentId((lsid.index() + 1) % (pagesPerPartition_ / pagesPerSegment_), lsid.partition());
}

KangarooLog::~KangarooLog() {
  killThread_ = true;
  writeSetsCv_.notify_all();
  flushLogCv_.notify_all();
  for (uint64_t i = 0; i < numThreads_; i++) {
    logToSetsThreads_[i].join();
  }
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    delete index_[i];
  }
  delete index_;
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
    throw std::invalid_argument(folly::sformat(
        "logSize must be a multiple of segmentSize. "
        "logSize:{}, segmentSize: {}.",
        logSize,
        segmentSize));
  }

  if (numIndexPartitionSlots == 0) {
    throw std::invalid_argument(folly::sformat(
          "log must have a greater than 0 number of buckets"
    ));
  }

  if (logPhysicalPartitions == 0) {
    throw std::invalid_argument(folly::sformat(
          "number physical partitions needs to be greater than 0"
          ));
  }

  if (numIndexPartitionSlots == 0) {
    throw std::invalid_argument(folly::sformat(
          "need more than 0 index slots"));
  }

  if (logIndexPartitions % logPhysicalPartitions != 0) {
    throw std::invalid_argument(folly::sformat(
          "the number of index partitions must be a multiple of the physical partitions"
    ));
  }

  if (logSize / logPhysicalPartitions % readSize != 0) {
    throw std::invalid_argument(folly::sformat(
          "Phycial partition size must be a multiple of read size"
    ));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  return *this;
}

KangarooLog::KangarooLog(Config&& config, ValidConfigTag)
    : pageSize_{config.readSize},
      segmentSize_{config.segmentSize},
      logBaseOffset_{config.logBaseOffset},
      logSize_{config.logSize},
      pagesPerSegment_{segmentSize_ / pageSize_},
      numSegments_{logSize_ / segmentSize_},
      device_{*config.device},
      logIndexPartitions_{config.logIndexPartitions},
      index_{new LogIndex*[logIndexPartitions_]},
      logPhysicalPartitions_{config.logPhysicalPartitions},
      physicalPartitionSize_{logSize_ / logPhysicalPartitions_},
      pagesPerPartition_{physicalPartitionSize_ / pageSize_},
      segmentsPerPartition_{pagesPerPartition_ / pagesPerSegment_},
      nextLsidsToClean_{std::make_unique<LogSegmentId[]>(logPhysicalPartitions_)},
      logSegmentBuffers_{new Buffer[logPhysicalPartitions_]},
      numThreads_{config.mergeThreads},
      nextCleaningPartition_{std::make_unique<uint64_t[]>(numThreads_)},
      currentLogSegments_{new KangarooLogSegment*[logPhysicalPartitions_]},
      setNumberCallback_{config.setNumberCallback},
      setMultiInsertCb_{config.setMultiInsertCallback},
      threshold_{config.threshold} {
  XLOGF(INFO,
        "Kangaroo Log created: size: {}, read size: {}, segment size: {}, base offset: {}",
        logSize_,
        pageSize_,
        segmentSize_,
        logBaseOffset_);
  for (uint64_t i = 0; i < logIndexPartitions_; i++) {
    index_[i] = new LogIndex(config.numIndexPartitionSlots, setNumberCallback_);
  }
  logSegmentMutexs_ = std::make_unique<folly::SharedMutex[]>(logPhysicalPartitions_);
  logIndexMutexs_ = std::make_unique<folly::SharedMutex[]>(logIndexPartitions_);
  reset();
  logToSetsThreads_.reserve(numThreads_);
  for (uint64_t i = 0; i < numThreads_; i++) {
    logToSetsThreads_.push_back(std::thread(&KangarooLog::cleanSegmentsLoop, this, i));
  }
}

bool KangarooLog::shouldWakeCompaction(uint64_t threadId) {
  for (uint64_t i = 0; i < logPhysicalPartitions_; i++) {
    uint64_t partition = (i + nextCleaningPartition_[threadId]) % logPhysicalPartitions_;
    if (partition % numThreads_ == threadId) {
      std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[partition]};
      LogSegmentId currentLsid = currentLogSegments_[partition]->getLogSegmentId();
      // may want to stay ahead by more than 1 Lsid but should work for now
      if (getNextLsid(currentLsid) == nextLsidsToClean_[partition]) {
        nextCleaningPartition_[threadId] = partition;
        return true;
      }
    }
  }
  // return true to kill thread if object is destructed
  return killThread_;
}

void KangarooLog::moveBucket(HashedKey hk) {
  ObjectInfo oi{hk};
  Buffer buffer;
  BufferView valueView;
  LogBucket* page;
  LogPageId lpid;
  KangarooBucketId bid = setNumberCallback_(hk.keyHash());
  uint64_t indexPartition = getIndexPartition(hk);
  LogIndex::BucketIterator indexIt;

  {
    std::shared_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
    indexIt = index_[indexPartition]->getHashBucketIterator(hk);
  }
  NextSetItemInLogCallback cb = [&](){
    if (indexIt.done()) {
      oi.status = Status::NotFound;
      return oi;
    }

    uint32_t tag;
    {
      std::unique_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
      oi.hits = indexIt.hits();
      tag = indexIt.tag();
      lpid = index_[indexPartition]->findAndRemove(bid, indexIt.tag());
    }
    if (!lpid.isValid()) {
      std::shared_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
      indexIt = index_[indexPartition]->getNext(indexIt);
      oi.status = Status::Retry;
      return oi;
    }
    
    buffer = readLogPage(lpid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      oi.status = Status::DeviceError;
      return oi;
    }
    page = reinterpret_cast<LogBucket*>(buffer.data());
  
    oi.status = page->findTag(tag, oi.key, oi.value);
    if (oi.status != Status::Ok) {
      indexIt = index_[indexPartition]->getNext(indexIt);
      oi.status = Status::Retry;
    } else if (setNumberCallback_(oi.key.keyHash()) != bid) {
      {
        std::unique_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
        index_[indexPartition]->insert(oi.key, lpid, oi.hits);
      }
      oi.status = Status::Retry;
      indexIt = index_[indexPartition]->getNext(indexIt);
      return oi;
    }
    oi.status = Status::Ok;
    indexIt = index_[indexPartition]->getNext(indexIt);
    return oi;
  };
  
  ReadmitCallback readmit = [&](ObjectInfo oi){
    uint64_t physicalPartition = getPhysicalPartition(lpid);
    /*if (nextLsidsToClean_[physicalPartition] != getSegmentId(lpid) && oi.hits) {
      std::unique_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
      index_[indexPartition]->insert(oi.key, lpid);
    }*/
  };

  setMultiInsertCb_(hk, cb, readmit);
}

void KangarooLog::cleanSegment(LogSegmentId lsid) {
  {
    std::shared_lock<folly::SharedMutex> segmentLock{getMutexFromSegment(lsid)};
    auto buf = readLogSegment(lsid);
    if (buf.isNull()) {
      ioErrorCount_.inc();
      return;
    }

    auto log_seg = KangarooLogSegment(segmentSize_, pageSize_, 
        lsid, pagesPerPartition_, buf.mutableView(), false);
    auto it = log_seg.getFirst();
    while (!it.done()) {
      uint64_t indexPartition = getIndexPartition(HashedKey(it.key()));
      LogPageId lpid;
      {
        std::shared_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
        lpid = index_[indexPartition]->lookup(it.key(), false);
      }
      if (!lpid.isValid()) {
        it = log_seg.getNext(it);
        continue;
      }

      // best effort count (remove could come and decrease 
      // but that should be rare)
      uint64_t count;
      {
        std::shared_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
        count = index_[indexPartition]->countBucket(it.key());
      }
      if (count < threshold_) {
        // evict key because set doesn't meet threshold 
        {
          std::unique_lock<folly::SharedMutex> indexLock{logIndexMutexs_[indexPartition]};
          index_[indexPartition]->remove(it.key());
        }
      } else {
        moveBucket(it.key());
      }
      it = log_seg.getNext(it);
    }
  }
}

void KangarooLog::cleanSegmentsLoop(uint64_t threadId) {
  while (true) {
    {
      std::unique_lock<std::mutex> lock{writeSetsMutex_};
      writeSetsCv_.wait(lock, [&](){ return shouldWakeCompaction(threadId); });
    }
    if (killThread_) {
      return;
    }
    writeSetsCv_.notify_all();
    cleanSegment(nextLsidsToClean_[nextCleaningPartition_[threadId]]);
    nextLsidsToClean_[nextCleaningPartition_[threadId]] = getNextLsid(nextLsidsToClean_[nextCleaningPartition_[threadId]]);
    nextCleaningPartition_[threadId]++;
    // wake up any insert requests waiting for space
    flushLogCv_.notify_all();
  }
}
  
Status KangarooLog::lookup(HashedKey hk, Buffer& value) {
  uint64_t indexPartition = getIndexPartition(hk);
  LogPageId lpid;
  {
    std::shared_lock<folly::SharedMutex> lock{logIndexMutexs_[indexPartition]};
    lpid = index_[indexPartition]->lookup(hk, true);
  }
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

Status KangarooLog::insert(HashedKey hk,
              BufferView value) {
  LogPageId lpid;
  LogSegmentId lsid;
  uint64_t physicalPartition = getPhysicalPartition(hk);
  insertCount_.inc();
  {
    // logSegment handles concurrent inserts
    // lock to prevent write out of segment
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[physicalPartition]};
    lpid = currentLogSegments_[physicalPartition]->insert(hk, value);
    if (!lpid.isValid()) {
      // need to flush segment using lsid
      lsid = currentLogSegments_[physicalPartition]->getLogSegmentId();
    }
  }
 
  if (lpid.isValid()) {
    uint64_t indexPartition = getIndexPartition(hk);
    std::unique_lock<folly::SharedMutex> lock{logIndexMutexs_[indexPartition]};
    auto ret = index_[indexPartition]->insert(hk, lpid);
    return ret;
  }
  
  if (flushLogSegment(lsid)) { 
    return insert(hk, value);
  } else {
    return Status::Rejected;
  }
}

Status KangarooLog::remove(HashedKey hk) {
  uint64_t indexPartition = getIndexPartition(hk);
  removeCount_.inc();

  Status status;
  
  {
    std::unique_lock<folly::SharedMutex> lock{logIndexMutexs_[indexPartition]};
    status = index_[indexPartition]->remove(hk);
  }

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
    Buffer& value, LogPageId lpid) {
  uint64_t partition = getPhysicalPartition(hk);
  BufferView view;
  {
    std::shared_lock<folly::SharedMutex> lock{logSegmentMutexs_[partition]};
    if (!isBuffered(lpid, partition)) {
      return Status::Retry;
    }
    view = currentLogSegments_[partition]->find(hk, lpid);
    if (view.isNull()) {
      return Status::NotFound;
    }
    value = Buffer{view};
  }
  return Status::Ok;
}

bool KangarooLog::isBuffered(LogPageId lpid, uint64_t partition) {
  return getSegmentId(lpid) == currentLogSegments_[partition]->getLogSegmentId();
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

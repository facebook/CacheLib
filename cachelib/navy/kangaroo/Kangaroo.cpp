#include <chrono>
#include <mutex>
#include <shared_mutex>

#include <folly/Format.h>

#include "cachelib/navy/kangaroo/Kangaroo.h"
#include "cachelib/navy/kangaroo/KangarooLog.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
constexpr uint64_t kMinSizeDistribution = 64;
constexpr double kSizeDistributionGranularityFactor = 1.25;
} // namespace

constexpr uint32_t Kangaroo::kFormatVersion;

Kangaroo::Config& Kangaroo::Config::validate() {
  if (totalSetSize < bucketSize) {
    throw std::invalid_argument(
        folly::sformat("cache size: {} cannot be smaller than bucket size: {}",
                       totalSetSize,
                       bucketSize));
  }

  if (!folly::isPowTwo(bucketSize)) {
    throw std::invalid_argument(
        folly::sformat("invalid bucket size: {}", bucketSize));
  }

  if (totalSetSize > uint64_t{bucketSize} << 32) {
    throw std::invalid_argument(folly::sformat(
        "Can't address big hash with 32 bits. Cache size: {}, bucket size: {}",
        totalSetSize,
        bucketSize));
  }

  if (cacheBaseOffset % bucketSize != 0 || totalSetSize % bucketSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "cacheBaseOffset and totalSetSize need to be a multiple of bucketSize. "
        "cacheBaseOffset: {}, totalSetSize:{}, bucketSize: {}.",
        cacheBaseOffset,
        totalSetSize,
        bucketSize));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  if (rripBitVector == nullptr) {
    throw std::invalid_argument("need a RRIP bit vector");
  }

  if (bloomFilter && bloomFilter->numFilters() != numBuckets()) {
    throw std::invalid_argument(
        folly::sformat("bloom filter #filters mismatch #buckets: {} vs {}",
                       bloomFilter->numFilters(),
                       numBuckets()));
  }

  if (logConfig.logSize > 0 && avgSmallObjectSize == 0) {
    throw std::invalid_argument(
        folly::sformat("Need an avgSmallObjectSize for the log"));
  }
  return *this;
}

Kangaroo::Kangaroo(Config&& config)
    : Kangaroo{std::move(config.validate()), ValidConfigTag{}} {}

Kangaroo::Kangaroo(Config&& config, ValidConfigTag)
    : destructorCb_{[this, cb = std::move(config.destructorCb)](
                        BufferView key,
                        BufferView value,
                        DestructorEvent event) {
        sizeDist_.removeSize(key.size() + value.size());
        if (cb) {
          cb(key, value, event);
        }
      }},
      bucketSize_{config.bucketSize},
      cacheBaseOffset_{config.cacheBaseOffset},
      numBuckets_{config.numBuckets()},
      bloomFilter_{std::move(config.bloomFilter)},
      bitVector_{std::move(config.rripBitVector)},
      device_{*config.device},
      sizeDist_{kMinSizeDistribution, bucketSize_,
                kSizeDistributionGranularityFactor} {
  XLOGF(INFO,
        "Kangaroo created: buckets: {}, bucket size: {}, base offset: {}",
        numBuckets_,
        bucketSize_,
        cacheBaseOffset_);
  if (config.logConfig.logSize) {
    SetNumberCallback cb = [&](uint64_t hk) {return getKangarooBucketIdFromHash(hk);};
    config.logConfig.setNumberCallback = cb;
    config.logConfig.logIndexPartitions = config.logIndexPartitionsPerPhysical * config.logConfig.logPhysicalPartitions;
    uint64_t bytesPerIndex = config.logConfig.logSize / config.logConfig.logIndexPartitions;
    config.logConfig.numIndexPartitionSlots = (bytesPerIndex / config.avgSmallObjectSize) * LogIndexOverhead;
    config.logConfig.device = config.device;
    config.logConfig.setMultiInsertCallback = [&](HashedKey hk, 
        NextSetItemInLogCallback cb, ReadmitCallback readmit) { return insertMultipleObjectsToKangarooBucket(hk, cb, readmit); };
    log_ = std::make_unique<KangarooLog>(std::move(config.logConfig));
  }
  reset();
}

void Kangaroo::reset() {
  XLOG(INFO, "Reset Kangaroo");
  generationTime_ = getSteadyClock();

  if (bloomFilter_) {
    bloomFilter_->reset();
  }

  itemCount_.set(0);
  insertCount_.set(0);
  succInsertCount_.set(0);
  lookupCount_.set(0);
  succLookupCount_.set(0);
  removeCount_.set(0);
  succRemoveCount_.set(0);
  evictionCount_.set(0);
  logicalWrittenCount_.set(0);
  physicalWrittenCount_.set(0);
  ioErrorCount_.set(0);
  bfFalsePositiveCount_.set(0);
  bfProbeCount_.set(0);
  checksumErrorCount_.set(0);
  sizeDist_.reset();
}

double Kangaroo::bfFalsePositivePct() const {
  const auto probes = bfProbeCount_.get();
  if (bloomFilter_ && probes > 0) {
    return 100.0 * bfFalsePositiveCount_.get() / probes;
  } else {
    return 0;
  }
}

void Kangaroo::insertMultipleObjectsToKangarooBucket(HashedKey hk,
        NextSetItemInLogCallback cb, ReadmitCallback readmit) {
  const auto bid = getKangarooBucketId(hk);
  insertCount_.inc();


  unsigned int removed{0};
  unsigned int evicted{0};
    
  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    auto buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return; 
    }

    auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bucket->reorder([&](uint32_t keyIdx) {return bvGetHit(bid, keyIdx);});
    bitVector_->clear(bid.index());
    
    ObjectInfo oi = cb();
    while (oi.status == Status::Ok || oi.status == Status::Retry) {
      if (oi.status != Status::Retry && bucket->isSpace(oi.key, oi.value, oi.hits)) {
        removed = bucket->remove(oi.key, destructorCb_);
        evicted = bucket->insert(oi.key, oi.value, oi.hits, destructorCb_);
        sizeDist_.addSize(oi.key.key().size() + oi.value.size());
        logicalWrittenCount_.add(oi.key.key().size() + oi.value.size());
        setInsertCount_.inc();
        itemCount_.sub(evicted + removed);
        logItemCount_.sub(1);
        setItemCount_.sub(evicted + removed);
        setItemCount_.inc();
        evictionCount_.add(evicted);
        succInsertCount_.inc();
      } else if (oi.status != Status::Retry) {
        if (oi.hits) {
          //readmit(oi);
          readmitInsertCount_.inc();
        }
      }
      oi = cb();
    }
        
    const auto res = writeBucket(bid, buffer.mutableView());
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return;
    }

    if (bloomFilter_) {
      if (removed + evicted == 0 && bloomFilter_->getInitBit(bid.index())) {
        // In case nothing was removed or evicted, we can just add
        bloomFilter_->set(bid.index(), hk.keyHash());
      } else {
        bfRebuild(bid, bucket);
      }
    }
  }

  physicalWrittenCount_.add(bucketSize_);
  return;
}

void Kangaroo::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bh_items", itemCount_.get());
  visitor("navy_bh_inserts", insertCount_.get());
  visitor("navy_bh_succ_inserts", succInsertCount_.get());
  visitor("navy_bh_lookups", lookupCount_.get());
  visitor("navy_bh_succ_lookups", succLookupCount_.get());
  visitor("navy_bh_removes", removeCount_.get());
  visitor("navy_bh_succ_removes", succRemoveCount_.get());
  visitor("navy_bh_evictions", evictionCount_.get());
  visitor("navy_bh_logical_written", logicalWrittenCount_.get());
  visitor("navy_bh_physical_written", physicalWrittenCount_.get());
  visitor("navy_bh_io_errors", ioErrorCount_.get());
  visitor("navy_bh_bf_false_positive_pct", bfFalsePositivePct());
  visitor("navy_bh_checksum_errors", checksumErrorCount_.get());
  auto snapshot = sizeDist_.getSnapshot();
  for (auto& kv : snapshot) {
    auto statName = folly::sformat("navy_bh_approx_bytes_in_size_{}", kv.first);
    visitor(statName.c_str(), kv.second);
  }
}

void Kangaroo::persist(RecordWriter& rw) {
  XLOG(INFO, "Starting kangaroo persist");
  serialization::BigHashPersistentData pd;
  pd.version = kFormatVersion;
  pd.generationTime = generationTime_.count();
  pd.itemCount = itemCount_.get();
  pd.bucketSize = bucketSize_;
  pd.cacheBaseOffset = cacheBaseOffset_;
  pd.numBuckets = numBuckets_;
  pd.sizeDist = sizeDist_.getSnapshot();
  serializeProto(pd, rw);

  if (bloomFilter_) {
    bloomFilter_->persist(rw);
  }

  XLOG(INFO, "Finished kangaroo persist");
}

bool Kangaroo::recover(RecordReader& rr) {
  XLOG(INFO, "Starting kangaroo recovery");
  try {
    auto pd = deserializeProto<serialization::BigHashPersistentData>(rr);
    if (pd.version != kFormatVersion) {
      throw std::logic_error{
          folly::sformat("invalid format version {}, expected {}",
                         pd.version,
                         kFormatVersion)};
    }

    auto configEquals =
        static_cast<uint64_t>(pd.bucketSize) == bucketSize_ &&
        static_cast<uint64_t>(pd.cacheBaseOffset) == cacheBaseOffset_ &&
        static_cast<uint64_t>(pd.numBuckets) == numBuckets_;
    if (!configEquals) {
      auto configStr = serializeToJson(pd);
      XLOGF(ERR, "Recovery config: {}", configStr.c_str());
      throw std::logic_error{"config mismatch"};
    }

    generationTime_ = std::chrono::nanoseconds{pd.generationTime};
    itemCount_.set(pd.itemCount);
    sizeDist_ = SizeDistribution{pd.sizeDist};
    if (bloomFilter_) {
      bloomFilter_->recover(rr);
    }
  } catch (const std::exception& e) {
    XLOGF(ERR, "Exception: {}", e.what());
    XLOG(ERR, "Failed to recover kangaroo. Resetting cache.");

    reset();
    return false;
  }
  XLOG(INFO, "Finished kangaroo recovery");
  return true;
}

Status Kangaroo::insert(HashedKey hk,
                       BufferView value,
                       InsertOptions /* opt */) {
  const auto bid = getKangarooBucketId(hk);
  insertCount_.inc();

  /*if (insertCount_.get() % 1000000 == 0) {
    XLOG(INFO, "Insert Counters");
    XLOG(INFO, "\t total {}", succInsertCount_.get());
    XLOG(INFO, "\t log {}", logInsertCount_.get());
    XLOG(INFO, "\t set {}", setInsertCount_.get());
    XLOG(INFO, "\t readmission ", readmitInsertCount_.get());
    
    XLOG(INFO, "Hit Counters");
    XLOG(INFO, "\t total", succLookupCount_.get(), "/ ", lookupCount_.get());
    XLOG(INFO, "\t log {}", logHits_.get());
    XLOG(INFO, "\t set {}", setHits_.get());

    XLOG(INFO, "# Items Counters");
    XLOG(INFO, "\t total {}", itemCount_.get());
    XLOG(INFO, "\t log {}", logItemCount_.get());
    XLOG(INFO, "\t set {}", setItemCount_.get());
  }*/
  
  if (log_) {
    Status ret = log_->insert(hk, value);
    if (ret == Status::Ok) {
      sizeDist_.addSize(hk.key().size() + value.size());
    }
    succInsertCount_.inc();
    logInsertCount_.inc();
    itemCount_.inc();
    logItemCount_.inc();
    return ret;
  }


  unsigned int removed{0};
  unsigned int evicted{0};
  bool space;

  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    auto buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bucket->reorder([&](uint32_t keyIdx) {return bvGetHit(bid, keyIdx);});
    space = bucket->isSpace(hk, value, 0);
    if (!space) {
      // no need to rewrite bucket
      removed = 0;
      evicted = 1;
    } else {
      bitVector_->clear(bid.index());
      removed = bucket->remove(hk, destructorCb_);
      evicted = bucket->insert(hk, value, 0, destructorCb_);
    }

    if (space) {
      const auto res = writeBucket(bid, buffer.mutableView());
      if (!res) {
        if (bloomFilter_) {
          bloomFilter_->clear(bid.index());
        }
        ioErrorCount_.inc();
        return Status::DeviceError;
      }
    }

    if (space && bloomFilter_) {
      if (removed + evicted == 0 && bloomFilter_->getInitBit(bid.index())) {
        // In case nothing was removed or evicted, we can just add
        bloomFilter_->set(bid.index(), hk.keyHash());
      } else {
        bfRebuild(bid, bucket);
      }
    }
  }

  sizeDist_.addSize(hk.key().size() + value.size());
  itemCount_.add(1);
  setItemCount_.inc();
  itemCount_.sub(evicted + removed);
  setItemCount_.sub(evicted + removed);
  evictionCount_.add(evicted);
  logicalWrittenCount_.add(hk.key().size() + value.size());
  setInsertCount_.inc();
  if (space) {
    // otherwise was not written
    physicalWrittenCount_.add(bucketSize_);
  }
  succInsertCount_.inc();
  return Status::Ok;
}

Status Kangaroo::lookup(HashedKey hk, Buffer& value) {
  const auto bid = getKangarooBucketId(hk);
  lookupCount_.inc();

  // first check log if it exists
  if (log_) {
    Status ret = log_->lookup(hk, value);
    if (ret == Status::Ok) {
      succLookupCount_.inc();
      logHits_.inc();
      return ret;
    }
  }

  RripBucket* bucket{nullptr};
  Buffer buffer;
  BufferView valueView;
  // scope of the lock is only needed until we read and mutate state for the
  // bucket. Once the bucket is read, the buffer is local and we can find
  // without holding the lock.
  {
    std::shared_lock<folly::SharedMutex> lock{getMutex(bid)};

    if (bfReject(bid, hk.keyHash())) {
      return Status::NotFound;
    }

    buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bfBuildUninitialized(bid, bucket);

    /* TODO: moving this inside lock could cause performance problem */
    valueView = bucket->find(hk, [&](uint32_t keyIdx) {bvSetHit(bid, keyIdx);});
  }
  
  if (valueView.isNull()) {
    bfFalsePositiveCount_.inc();
    return Status::NotFound;
  }
  value = Buffer{valueView};
  succLookupCount_.inc();
  setHits_.inc();
  return Status::Ok;
}

Status Kangaroo::remove(HashedKey hk) {
  const auto bid = getKangarooBucketId(hk);
  removeCount_.inc();

  if (log_) {
    Status ret = log_->remove(hk);
    if (ret == Status::Ok) {
      succRemoveCount_.inc();
      itemCount_.dec();
      logItemCount_.dec();
      return ret;
    }
  }

  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    if (bfReject(bid, hk.keyHash())) {
      return Status::NotFound;
    }

    auto buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bucket->reorder([&](uint32_t keyIdx) {return bvGetHit(bid, keyIdx);});

    if (!bucket->remove(hk, destructorCb_)) {
      bfBuildUninitialized(bid, bucket);
      bfFalsePositiveCount_.inc();
      return Status::NotFound;
    }

    const auto res = writeBucket(bid, buffer.mutableView());
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    if (bloomFilter_) {
      bfRebuild(bid, bucket);
    }
    bitVector_->clear(bid.index());
  }

  itemCount_.dec();
  setItemCount_.dec();

  // We do not bump logicalWrittenCount_ because logically a
  // remove operation does not write, but for Kangaroo, it does
  // incur physical writes.
  physicalWrittenCount_.add(bucketSize_);
  succRemoveCount_.inc();
  return Status::Ok;
}

bool Kangaroo::bfReject(KangarooBucketId bid, uint64_t keyHash) const {
  if (bloomFilter_) {
    bfProbeCount_.inc();
    if (!bloomFilter_->couldExist(bid.index(), keyHash)) {
      bfRejectCount_.inc();
      return true;
    }
  }
  return false;
}

bool Kangaroo::bvGetHit(KangarooBucketId bid, uint32_t keyIdx) const {
  if (bitVector_) {
    return bitVector_->get(bid.index(), keyIdx);
  }
  return false;
}

void Kangaroo::bvSetHit(KangarooBucketId bid, uint32_t keyIdx) const {
  if (bitVector_) {
    bitVector_->set(bid.index(), keyIdx);
  }
}

void Kangaroo::bfBuildUninitialized(KangarooBucketId bid, const RripBucket* bucket) {
  if (bloomFilter_ && !bloomFilter_->getInitBit(bid.index())) {
    bfRebuild(bid, bucket);
  }
}

void Kangaroo::bfRebuild(KangarooBucketId bid, const RripBucket* bucket) {
  XDCHECK(bloomFilter_);
  bloomFilter_->clear(bid.index());
  auto itr = bucket->getFirst();
  while (!itr.done()) {
    bloomFilter_->set(bid.index(), itr.keyHash());
    itr = bucket->getNext(itr);
  }
  bloomFilter_->setInitBit(bid.index());
}

void Kangaroo::flush() {
  XLOG(INFO, "Flush big hash");
  device_.flush();
}

Buffer Kangaroo::readBucket(KangarooBucketId bid) {
  auto buffer = device_.makeIOBuffer(bucketSize_);
  XDCHECK(!buffer.isNull());

  const bool res =
      device_.read(getBucketOffset(bid), buffer.size(), buffer.data());
  if (!res) {
    return {};
  }

  auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());

  const auto checksumSuccess =
      RripBucket::computeChecksum(buffer.view()) == bucket->getChecksum();
  // We can only know for certain this is a valid checksum error if bloom filter
  // is already initialized. Otherwise, it could very well be because we're
  // reading the bucket for the first time.
  if (!checksumSuccess && bloomFilter_ &&
      bloomFilter_->getInitBit(bid.index())) {
    checksumErrorCount_.inc();
  }

  if (!checksumSuccess || static_cast<uint64_t>(generationTime_.count()) !=
                              bucket->generationTime()) {
    RripBucket::initNew(buffer.mutableView(), generationTime_.count());
  }
  return buffer;
}

bool Kangaroo::writeBucket(KangarooBucketId bid, MutableBufferView mutableView) {
  auto* bucket = reinterpret_cast<RripBucket*>(mutableView.data());
  bucket->setChecksum(RripBucket::computeChecksum(toView(mutableView)));
  return device_.write(
      getBucketOffset(bid), mutableView.size(), mutableView.data());
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

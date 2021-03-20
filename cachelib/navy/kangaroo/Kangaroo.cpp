#include "cachelib/navy/kangaroo/Kangaroo.h"

#include <folly/Format.h>

#include <chrono>
#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/kangaroo/KangarooLog.h"
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
                kSizeDistributionGranularityFactor},
      thresholdSizeDist_{10, bucketSize_, 10},
      thresholdNumDist_{1, 25, 1} {
  XLOGF(INFO,
        "Kangaroo created: buckets: {}, bucket size: {}, base offset: {}",
        numBuckets_,
        bucketSize_,
        cacheBaseOffset_);
  if (config.logConfig.logSize) {
    SetNumberCallback cb = [&](uint64_t hk) {
      return getKangarooBucketIdFromHash(hk);
    };
    config.logConfig.setNumberCallback = cb;
    config.logConfig.logIndexPartitions =
        config.logIndexPartitionsPerPhysical *
        config.logConfig.logPhysicalPartitions;
    config.logConfig.device = config.device;
    config.logConfig.setMultiInsertCallback =
        [&](std::vector<std::unique_ptr<ObjectInfo>>& ois,
            ReadmitCallback readmit) {
          return insertMultipleObjectsToKangarooBucket(ois, readmit);
        };
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

void Kangaroo::insertMultipleObjectsToKangarooBucket(
    std::vector<std::unique_ptr<ObjectInfo>>& ois, ReadmitCallback readmit) {
  const auto bid = getKangarooBucketId(ois[0]->key);
  insertCount_.inc();
  multiInsertCalls_.inc();

  uint64_t insertCount = 0;
  uint64_t evictCount = 0;
  uint64_t removedCount = 0;

  uint64_t passedItemSize = 0;
  uint64_t passedCount = 0;

  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    auto buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return;
    }

    auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
    bucket->reorder([&](uint32_t keyIdx) { return bvGetHit(bid, keyIdx); });
    bitVector_->clear(bid.index());

    for (auto& oi : ois) {
      passedItemSize += oi->key.key().size() + oi->value.size();
      passedCount++;

      if (bucket->isSpace(oi->key, oi->value.view(), oi->hits)) {
        removedCount += bucket->remove(oi->key, destructorCb_);
        evictCount +=
            bucket->insert(oi->key, oi->value.view(), oi->hits, destructorCb_);
        sizeDist_.addSize(oi->key.key().size() + oi->value.size());
        insertCount++;
      } else {
        readmit(oi);
        readmitInsertCount_.inc();
      }
    }

    const auto res = writeBucket(bid, std::move(buffer));
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return;
    }

    if (bloomFilter_) {
      bfRebuild(bid, bucket);
    }
  }

  thresholdSizeDist_.addSize(passedItemSize);
  thresholdNumDist_.addSize(passedCount * 2);

  setInsertCount_.add(insertCount);
  itemCount_.sub(evictCount + removedCount);
  logItemCount_.sub(insertCount);
  setItemCount_.sub(evictCount + removedCount);
  setItemCount_.add(insertCount);
  evictionCount_.add(evictCount);
  succInsertCount_.add(insertCount);

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
  uint64_t logBytesWritten = (log_) ? log_->getBytesWritten() : 0;
  visitor("navy_bh_physical_written",
          physicalWrittenCount_.get() + logBytesWritten);
  visitor("navy_bh_io_errors", ioErrorCount_.get());
  visitor("navy_bh_bf_false_positive_pct", bfFalsePositivePct());
  visitor("navy_bh_checksum_errors", checksumErrorCount_.get());
  if (log_) {
    visitor("navy_klog_false_positive_pct", log_->falsePositivePct());
    visitor("navy_klog_fragmentation_pct", log_->fragmentationPct());
    visitor("navy_klog_extra_reads_pct", log_->extraReadsPct());
  }
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
  *pd.sizeDist_ref() = sizeDist_.getSnapshot();
  serializeProto(pd, rw);

  if (bloomFilter_) {
    bloomFilter_->persist<ProtoSerializer>(rw);
    XLOG(INFO, "bloom filter persist done");
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
    sizeDist_ = SizeDistribution{*pd.sizeDist_ref()};
    if (bloomFilter_) {
      bloomFilter_->recover<ProtoSerializer>(rr);
      XLOG(INFO, "Recovered bloom filter");
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

Status Kangaroo::insert(HashedKey hk, BufferView value) {
  const auto bid = getKangarooBucketId(hk);
  insertCount_.inc();

  if (log_) {
    Status ret = log_->insert(hk, value);
    if (ret == Status::Ok) {
      sizeDist_.addSize(hk.key().size() + value.size());
      succInsertCount_.inc();
    }
    logicalWrittenCount_.add(hk.key().size() + value.size());
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
    bucket->reorder([&](uint32_t keyIdx) { return bvGetHit(bid, keyIdx); });
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
      const auto res = writeBucket(bid, Buffer(buffer.view(), bucketSize_));
      if (!res) {
        if (bloomFilter_) {
          bloomFilter_->clear(bid.index());
        }
        ioErrorCount_.inc();
        return Status::DeviceError;
      }
    }

    if (space && bloomFilter_) {
      if (removed + evicted == 0) {
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

    /* TODO: moving this inside lock could cause performance problem */
    valueView =
        bucket->find(hk, [&](uint32_t keyIdx) { bvSetHit(bid, keyIdx); });
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
    bucket->reorder([&](uint32_t keyIdx) { return bvGetHit(bid, keyIdx); });

    if (!bucket->remove(hk, destructorCb_)) {
      bfFalsePositiveCount_.inc();
      return Status::NotFound;
    }

    const auto res = writeBucket(bid, std::move(buffer));
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

bool Kangaroo::couldExist(HashedKey hk) {
  const auto bid = getKangarooBucketId(hk);
  bool canExist = false;

  if (log_) {
    canExist = log_->couldExist(hk);
  }

  if (!canExist) {
    std::shared_lock<folly::SharedMutex> lock{getMutex(bid)};
    canExist = !bfReject(bid, hk.keyHash());
  }

  // the caller is not likely to issue a subsequent lookup when we return
  // false. hence tag this as a lookup. If we return the key can exist, the
  // caller will perform a lookupAsync and will be counted within lookup api.
  if (!canExist) {
    lookupCount_.inc();
  }
  return canExist;
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

void Kangaroo::bfRebuild(KangarooBucketId bid, const RripBucket* bucket) {
  XDCHECK(bloomFilter_);
  bloomFilter_->clear(bid.index());
  auto itr = bucket->getFirst();
  while (!itr.done()) {
    bloomFilter_->set(bid.index(), itr.keyHash());
    itr = bucket->getNext(itr);
  }
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
  if (!checksumSuccess && bloomFilter_) {
    checksumErrorCount_.inc();
  }

  if (!checksumSuccess || static_cast<uint64_t>(generationTime_.count()) !=
                              bucket->generationTime()) {
    RripBucket::initNew(buffer.mutableView(), generationTime_.count());
  }
  return buffer;
}

bool Kangaroo::writeBucket(KangarooBucketId bid, Buffer buffer) {
  auto* bucket = reinterpret_cast<RripBucket*>(buffer.data());
  bucket->setChecksum(RripBucket::computeChecksum(buffer.view()));
  return device_.write(getBucketOffset(bid), std::move(buffer));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

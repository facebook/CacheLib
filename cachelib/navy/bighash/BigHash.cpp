#include "cachelib/navy/bighash/BigHash.h"

#include <folly/Format.h>

#include <chrono>
#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/bighash/Bucket.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/serialization/Serialization.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
constexpr uint64_t kMinSizeDistribution = 64;
constexpr double kSizeDistributionGranularityFactor = 1.25;
} // namespace

constexpr uint32_t BigHash::kFormatVersion;

BigHash::Config& BigHash::Config::validate() {
  if (cacheSize < bucketSize) {
    throw std::invalid_argument(
        folly::sformat("cache size: {} cannot be smaller than bucket size: {}",
                       cacheSize,
                       bucketSize));
  }

  if (!folly::isPowTwo(bucketSize)) {
    throw std::invalid_argument(
        folly::sformat("invalid bucket size: {}", bucketSize));
  }

  if (cacheSize > uint64_t{bucketSize} << 32) {
    throw std::invalid_argument(folly::sformat(
        "Can't address big hash with 32 bits. Cache size: {}, bucket size: {}",
        cacheSize,
        bucketSize));
  }

  if (cacheBaseOffset % bucketSize != 0 || cacheSize % bucketSize != 0) {
    throw std::invalid_argument(folly::sformat(
        "cacheBaseOffset and cacheSize need to be a multiple of bucketSize. "
        "cacheBaseOffset: {}, cacheSize:{}, bucketSize: {}.",
        cacheBaseOffset,
        cacheSize,
        bucketSize));
  }

  if (device == nullptr) {
    throw std::invalid_argument("device cannot be null");
  }

  if (bloomFilter && bloomFilter->numFilters() != numBuckets()) {
    throw std::invalid_argument(
        folly::sformat("bloom filter #filters mismatch #buckets: {} vs {}",
                       bloomFilter->numFilters(),
                       numBuckets()));
  }
  return *this;
}

BigHash::BigHash(Config&& config)
    : BigHash{std::move(config.validate()), ValidConfigTag{}} {}

BigHash::BigHash(Config&& config, ValidConfigTag)
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
      device_{*config.device},
      sizeDist_{kMinSizeDistribution, bucketSize_,
                kSizeDistributionGranularityFactor} {
  XLOGF(INFO,
        "BigHash created: buckets: {}, bucket size: {}, base offset: {}",
        numBuckets_,
        bucketSize_,
        cacheBaseOffset_);
  reset();
}

void BigHash::reset() {
  XLOG(INFO, "Reset BigHash");
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
  usedSizeBytes_.set(0);
}

double BigHash::bfFalsePositivePct() const {
  const auto probes = bfProbeCount_.get();
  if (bloomFilter_ && probes > 0) {
    return 100.0 * bfFalsePositiveCount_.get() / probes;
  } else {
    return 0;
  }
}

uint64_t BigHash::getMaxItemSize() const {
  auto itemOverhead = BucketStorage::slotSize(sizeof(details::BucketEntry));
  return bucketSize_ - sizeof(Bucket) - itemOverhead;
}

void BigHash::getCounters(const CounterVisitor& visitor) const {
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
  visitor("navy_bh_bf_lookups", bfProbeCount_.get());
  visitor("navy_bh_bf_rebuilds", bfRebuildCount_.get());
  visitor("navy_bh_checksum_errors", checksumErrorCount_.get());
  visitor("navy_bh_used_size_bytes", usedSizeBytes_.get());
  auto snapshot = sizeDist_.getSnapshot();
  for (auto& kv : snapshot) {
    auto statName = folly::sformat("navy_bh_approx_bytes_in_size_{}", kv.first);
    visitor(statName.c_str(), kv.second);
  }
}

void BigHash::persist(RecordWriter& rw) {
  XLOG(INFO, "Starting bighash persist");
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

  XLOG(INFO, "Finished bighash persist");
}

bool BigHash::recover(RecordReader& rr) {
  XLOG(INFO, "Starting bighash recovery");
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
    XLOG(ERR, "Failed to recover bighash. Resetting cache.");

    reset();
    return false;
  }
  XLOG(INFO, "Finished bighash recovery");
  return true;
}

Status BigHash::insert(HashedKey hk, BufferView value) {
  const auto bid = getBucketId(hk);
  insertCount_.inc();

  uint32_t removed{0};
  uint32_t evicted{0};

  uint32_t oldRemainingBytes = 0;
  uint32_t newRemainingBytes = 0;
  {
    std::unique_lock<folly::SharedMutex> lock{getMutex(bid)};
    auto buffer = readBucket(bid);
    if (buffer.isNull()) {
      ioErrorCount_.inc();
      return Status::DeviceError;
    }

    auto* bucket = reinterpret_cast<Bucket*>(buffer.data());
    oldRemainingBytes = bucket->remainingBytes();
    removed = bucket->remove(hk, destructorCb_);
    evicted = bucket->insert(hk, value, destructorCb_);
    newRemainingBytes = bucket->remainingBytes();

    // rebuild / fix the bloom filter before we move the buffer to do the
    // actual write
    if (bloomFilter_) {
      if (removed + evicted == 0) {
        // In case nothing was removed or evicted, we can just add
        bloomFilter_->set(bid.index(), hk.keyHash());
      } else {
        bfRebuild(bid, bucket);
      }
    }

    const auto res = writeBucket(bid, std::move(buffer));
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
  }

  if (oldRemainingBytes < newRemainingBytes) {
    usedSizeBytes_.sub(newRemainingBytes - oldRemainingBytes);
  } else {
    usedSizeBytes_.add(oldRemainingBytes - newRemainingBytes);
  }
  sizeDist_.addSize(hk.key().size() + value.size());
  itemCount_.add(1);
  itemCount_.sub(evicted + removed);
  evictionCount_.add(evicted);
  logicalWrittenCount_.add(hk.key().size() + value.size());
  physicalWrittenCount_.add(bucketSize_);
  succInsertCount_.inc();
  return Status::Ok;
}

bool BigHash::couldExist(HashedKey hk) {
  const auto bid = getBucketId(hk);
  bool canExist;
  {
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

Status BigHash::lookup(HashedKey hk, Buffer& value) {
  const auto bid = getBucketId(hk);
  lookupCount_.inc();

  Bucket* bucket{nullptr};
  Buffer buffer;
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

    bucket = reinterpret_cast<Bucket*>(buffer.data());
  }

  auto valueView = bucket->find(hk);
  if (valueView.isNull()) {
    bfFalsePositiveCount_.inc();
    return Status::NotFound;
  }
  value = Buffer{valueView};
  succLookupCount_.inc();
  return Status::Ok;
}

Status BigHash::remove(HashedKey hk) {
  const auto bid = getBucketId(hk);
  removeCount_.inc();

  uint32_t oldRemainingBytes = 0;
  uint32_t newRemainingBytes = 0;
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

    auto* bucket = reinterpret_cast<Bucket*>(buffer.data());
    oldRemainingBytes = bucket->remainingBytes();
    if (!bucket->remove(hk, destructorCb_)) {
      bfFalsePositiveCount_.inc();
      return Status::NotFound;
    }
    newRemainingBytes = bucket->remainingBytes();

    // We compute bloom filter before writing the bucket because when encryption
    // is enabled, we will "move" the bucket content into writeBucket().
    if (bloomFilter_) {
      bfRebuild(bid, bucket);
    }

    const auto res = writeBucket(bid, std::move(buffer));
    if (!res) {
      if (bloomFilter_) {
        bloomFilter_->clear(bid.index());
      }
      ioErrorCount_.inc();
      return Status::DeviceError;
    }
  }

  XDCHECK_LE(oldRemainingBytes, newRemainingBytes);
  usedSizeBytes_.sub(newRemainingBytes - oldRemainingBytes);
  itemCount_.dec();

  // We do not bump logicalWrittenCount_ because logically a
  // remove operation does not write, but for BigHash, it does
  // incur physical writes.
  physicalWrittenCount_.add(bucketSize_);
  succRemoveCount_.inc();
  return Status::Ok;
}

bool BigHash::bfReject(BucketId bid, uint64_t keyHash) const {
  if (bloomFilter_) {
    bfProbeCount_.inc();
    if (!bloomFilter_->couldExist(bid.index(), keyHash)) {
      bfRejectCount_.inc();
      return true;
    }
  }
  return false;
}

void BigHash::bfRebuild(BucketId bid, const Bucket* bucket) {
  bfRebuildCount_.inc();
  XDCHECK(bloomFilter_);
  bloomFilter_->clear(bid.index());
  auto itr = bucket->getFirst();
  while (!itr.done()) {
    bloomFilter_->set(bid.index(), itr.keyHash());
    itr = bucket->getNext(itr);
  }
}

void BigHash::flush() {
  XLOG(INFO, "Flush big hash");
  device_.flush();
}

Buffer BigHash::readBucket(BucketId bid) {
  auto buffer = device_.makeIOBuffer(bucketSize_);
  XDCHECK(!buffer.isNull());

  const bool res =
      device_.read(getBucketOffset(bid), buffer.size(), buffer.data());
  if (!res) {
    return {};
  }

  auto* bucket = reinterpret_cast<Bucket*>(buffer.data());

  const auto checksumSuccess =
      Bucket::computeChecksum(buffer.view()) == bucket->getChecksum();
  // TODO (T93631284) we only read a bucket if the bloom filter indicates that
  // the bucket could have the element. Hence, if check sum errors out and bloom
  // filter is enable, we could record the checksum error. However, doing so
  // could lead to false positives on check sum errors for buckets that were not
  // initialized (by writing to it), but were read due to bloom filter having a
  // false positive.  Hence, we can't differentiate between false positives and
  // real check sum errors due device failures
  //
  // if (!checksumSuccess && bloomFilter_) {
  //  checksumErrorCount_.inc();
  // }

  if (!checksumSuccess || static_cast<uint64_t>(generationTime_.count()) !=
                              bucket->generationTime()) {
    Bucket::initNew(buffer.mutableView(), generationTime_.count());
  }
  return buffer;
}

bool BigHash::writeBucket(BucketId bid, Buffer buffer) {
  auto* bucket = reinterpret_cast<Bucket*>(buffer.data());
  bucket->setChecksum(Bucket::computeChecksum(buffer.view()));
  return device_.write(getBucketOffset(bid), std::move(buffer));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

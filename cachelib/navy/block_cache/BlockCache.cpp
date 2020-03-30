
#include <algorithm>
#include <cstring>
#include <numeric>
#include <utility>

#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/block_cache/BlockCache.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
constexpr uint64_t kMinSizeDistribution = 64;
constexpr double kSizeDistributionGranularityFactor = 1.25;
} // namespace

constexpr uint32_t BlockCache::kFormatVersion;

BlockCache::Config& BlockCache::Config::validate() {
  XDCHECK_NE(scheduler, nullptr);
  if (!device || !evictionPolicy) {
    throw std::invalid_argument("missing required param");
  }
  if (!folly::isPowTwo(blockSize)) {
    throw std::invalid_argument("invalid block size");
  }
  if (regionSize % blockSize != 0) {
    throw std::invalid_argument("invalid region size");
  }
  if (regionSize > 256u << 20) {
    // We allocate region in memory to reclaim. Too large region will cause
    // problems: at least, long allocation times.
    throw std::invalid_argument("region is too large");
  }
  if (cacheSize <= 0) {
    throw std::invalid_argument("invalid size");
  }
  if (cacheSize > uint64_t{blockSize} << 32) {
    throw std::invalid_argument("can't address cache with 32 bits");
  }
  if (getNumRegions() < sizeClasses.size() + cleanRegionsPool) {
    throw std::invalid_argument("not enough space on device");
  }
  if (readBufferSize % blockSize != 0) {
    throw std::invalid_argument("invalid read buffer size");
  }
  for (auto& sc : sizeClasses) {
    if (sc == 0 || sc % blockSize != 0 || sc > regionSize) {
      throw std::invalid_argument(folly::sformat("invalid size class: {}", sc));
    }
  }
  return *this;
}

BlockCache::BlockCache(Config&& config)
    : BlockCache{std::move(config.validate()), ValidConfigTag{}} {}

BlockCache::BlockCache(Config&& config, ValidConfigTag)
    : config_{serializeConfig(config)},
      destructorCb_{std::move(config.destructorCb)},
      checksumData_{config.checksum},
      blockSize_{config.blockSize},
      readBufferSize_{config.getReadBufferSize()},
      regionManager_{
          config.getNumRegions(), config.regionSize,
          config.cacheBaseOffset, config.blockSize,
          *config.device,         config.cleanRegionsPool,
          *config.scheduler,      bindThis(&BlockCache::onRegionReclaim, *this),
          config.sizeClasses,     std::move(config.evictionPolicy),
          config.numInMemBuffers},
      allocator_{regionManager_, config.blockSize},
      reinsertionPolicy_{std::move(config.reinsertionPolicy)},
      sizeDist_{kMinSizeDistribution, config.regionSize,
                kSizeDistributionGranularityFactor} {
  XLOG(INFO, "Block cache created");
  XDCHECK_NE(readBufferSize_, 0u);
}

uint32_t BlockCache::serializedSize(uint32_t keySize, uint32_t valueSize) {
  return sizeof(EntryDesc) + keySize + valueSize;
}

Status BlockCache::insert(HashedKey hk, BufferView value, InsertOptions opt) {
  RelAddress addr;
  uint32_t slotSize = 0;
  RegionDescriptor desc{OpenStatus::Retry};

  std::tie(desc, slotSize, addr) = allocator_.allocate(
      serializedSize(hk.key().size(), value.size()), opt.permanent);
  switch (desc.status()) {
  case OpenStatus::Error:
    allocErrorCount_.inc();
    insertCount_.inc();
    return Status::Rejected;
  case OpenStatus::Ready:
    insertCount_.inc();
    break;
  case OpenStatus::Reclaimed:
  case OpenStatus::Retry:
    return Status::Retry;
  }
  // After allocation a region is opened for writing. Until we close it, the
  // region would not be reclaimed and index never gets an invalid entry.
  const auto status = writeEntry(addr, slotSize, hk, value);
  if (status == Status::Ok) {
    const auto lr =
        index_.insert(hk.keyHash(), encodeRelAddress(addr.add(slotSize)));
    // We replaced an existing key in the index
    if (lr.found()) {
      holeSizeTotal_.add(
          regionManager_.getRegionSlotSize(decodeRelAddress(lr.value()).rid()));
      holeCount_.inc();
      insertHashCollisionCount_.inc();
    }
    succInsertCount_.inc();
    sizeDist_.addSize(slotSize);
    if (reinsertionPolicy_) {
      reinsertionPolicy_->track(hk);
    }
  }
  allocator_.close(std::move(desc));
  return status;
}

Status BlockCache::lookup(HashedKey hk, Buffer& value) {
  const auto seqNumber = regionManager_.getSeqNumber();
  const auto lr = index_.lookup(hk.keyHash());
  if (!lr.found()) {
    lookupCount_.inc();
    return Status::NotFound;
  }
  // If relative address has offset 0, the entry actually belongs to the
  // previous region (this is address of its end). To compensate for this, we
  // subtract 1 before conversion and add after to relative address.
  auto addrEnd = decodeRelAddress(lr.value());
  // Between acquring @seqNumber and @openForRead reclamation may start. There
  // are two options what can happen in @openForRead:
  //  - Reclamation in progress and open will fail because access mask disables
  //    read and write,
  //  - Reclamation finished, sequence number increased and open will fail
  //    because of sequence number check. This means sequence number has to be
  //    increased when we finish reclamation.
  RegionDescriptor desc = regionManager_.openForRead(addrEnd.rid(), seqNumber);
  switch (desc.status()) {
  case OpenStatus::Ready: {
    auto status = readEntry(desc, addrEnd, hk, value);
    if (status == Status::Ok) {
      regionManager_.touch(addrEnd.rid());
      succLookupCount_.inc();
      if (reinsertionPolicy_) {
        reinsertionPolicy_->touch(hk);
      }
    }
    regionManager_.close(std::move(desc));
    lookupCount_.inc();
    return status;
  }
  case OpenStatus::Retry:
  case OpenStatus::Reclaimed:
    // If status is reclaimed, we may still have a chance of reading the
    // item, if it is eligible for re-insertion. So we retry.
    return Status::Retry;
  default:
    // Open region never returns other statuses than above
    XDCHECK(!"unreachable");
    return Status::DeviceError;
  }
}

Status BlockCache::remove(HashedKey hk) {
  removeCount_.inc();
  auto lr = index_.remove(hk.keyHash());
  if (lr.found()) {
    auto addr = decodeRelAddress(lr.value());
    holeSizeTotal_.add(regionManager_.getRegionSlotSize(addr.rid()));
    holeCount_.inc();
    succRemoveCount_.inc();
    if (reinsertionPolicy_) {
      reinsertionPolicy_->remove(hk);
    }
    return Status::Ok;
  }
  return Status::NotFound;
}

// Remove all region entries from index and invoke callback
// See @RegionEvictCallback for details
uint32_t BlockCache::onRegionReclaim(RegionId rid,
                                     uint32_t slotSize,
                                     BufferView buffer) {
  // Eviction callback guarantees are the following:
  //   - Every value inserted will get eviction callback at some point of
  //     time.
  //   - It is invoked only once per insertion.
  //
  // We do not guarantee time between remove and callback invocation. If a
  // value v1 was replaced with v2 user will get callbacks for both v1 and
  // v2 when they are evicted (in no particular order).
  uint32_t evictionCount = 0; // item that was evicted during reclaim
  uint32_t removedItem = 0;   // item that was previously removed
  auto& region = regionManager_.getRegion(rid);
  auto offset = region.getLastEntryEndOffset();
  while (offset > 0) {
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    const auto entrySize = slotSize > 0
                               ? slotSize
                               : allocator_.alignOnBlock(serializedSize(
                                     desc.keySize, desc.valueSize));
    HashedKey hk{
        BufferView{desc.keySize, entryEnd - sizeof(EntryDesc) - desc.keySize}};
    BufferView value{desc.valueSize, entryEnd - entrySize};

    const auto reinsertionRes =
        reinsertOrRemoveItem(hk, value, entrySize, RelAddress{rid, offset});
    switch (reinsertionRes) {
    case ReinsertionRes::kEvicted:
      evictionCount++;
      break;
    case ReinsertionRes::kRemoved:
      removedItem++;
      break;
    case ReinsertionRes::kReinserted:
      break;
    }

    if (destructorCb_ && reinsertionRes != ReinsertionRes::kReinserted) {
      destructorCb_(hk.key(),
                    value,
                    reinsertionRes == ReinsertionRes::kEvicted
                        ? DestructorEvent::Recycled
                        : DestructorEvent::Removed);
    }
    XDCHECK_GE(offset, entrySize);
    offset -= entrySize;
  }

  XDCHECK_GE(region.getNumItems(), evictionCount);
  holeCount_.sub(removedItem);
  holeSizeTotal_.sub(removedItem * regionManager_.getRegionSlotSize(rid));
  return evictionCount;
}

BlockCache::ReinsertionRes BlockCache::reinsertOrRemoveItem(
    HashedKey hk, BufferView value, uint32_t entrySize, RelAddress currAddr) {
  auto removeItem = [this, hk, entrySize, currAddr] {
    if (reinsertionPolicy_) {
      reinsertionPolicy_->remove(hk);
    }
    sizeDist_.removeSize(entrySize);
    if (index_.remove(hk.keyHash(), encodeRelAddress(currAddr))) {
      return ReinsertionRes::kEvicted;
    }
    return ReinsertionRes::kRemoved;
  };

  const auto lr = index_.lookup(hk.keyHash());
  if (!lr.found() || decodeRelAddress(lr.value()) != currAddr) {
    evictionLookupMissCounter_.inc();
    sizeDist_.removeSize(entrySize);
    return ReinsertionRes::kRemoved;
  }

  if (!reinsertionPolicy_ || !reinsertionPolicy_->shouldReinsert(hk)) {
    return removeItem();
  }

  RelAddress addr;
  uint32_t slotSize = 0;
  RegionDescriptor desc{OpenStatus::Retry};

  std::tie(desc, slotSize, addr) = allocator_.allocate(
      serializedSize(hk.key().size(), value.size()), false /* permanent */);
  switch (desc.status()) {
  case OpenStatus::Ready:
    break;
  case OpenStatus::Error:
    allocErrorCount_.inc();
    // fall-through
  case OpenStatus::Reclaimed:
  case OpenStatus::Retry:
    reinsertionErrorCount_.inc();
    return removeItem();
  }
  auto closeRegionGuard =
      folly::makeGuard([this, desc = std::move(desc)]() mutable {
        allocator_.close(std::move(desc));
      });

  // After allocation a region is opened for writing. Until we close it, the
  // region would not be reclaimed and index never gets an invalid entry.
  const auto status = writeEntry(addr, slotSize, hk, value);
  if (status != Status::Ok) {
    reinsertionErrorCount_.inc();
    return removeItem();
  }

  const auto replaced = index_.replace(hk.keyHash(),
                                       encodeRelAddress(addr.add(slotSize)),
                                       encodeRelAddress(currAddr));
  if (!replaced) {
    reinsertionErrorCount_.inc();
    return removeItem();
  }
  reinsertionCount_.inc();
  reinsertionBytes_.add(entrySize);
  return ReinsertionRes::kReinserted;
}

Status BlockCache::writeEntry(RelAddress addr,
                              uint32_t slotSize,
                              HashedKey hk,
                              BufferView value) {
  XDCHECK_EQ(slotSize % blockSize_, 0u);
  XDCHECK_LE(addr.offset() + slotSize, regionManager_.regionSize());
  auto buffer = regionManager_.makeIOBuffer(slotSize);

  // Copy descriptor and the key to the end
  size_t descOffset = buffer.size() - sizeof(EntryDesc);
  auto desc = new (buffer.data() + descOffset)
      EntryDesc(hk.key().size(), value.size(), hk.keyHash());
  if (checksumData_) {
    desc->cs = checksum(value);
  }

  buffer.copyFrom(descOffset - hk.key().size(), hk.key());
  buffer.copyFrom(0, value);

  if (!regionManager_.write(addr, std::move(buffer))) {
    return Status::DeviceError;
  }
  logicalWrittenCount_.add(hk.key().size() + value.size());
  return Status::Ok;
}

Status BlockCache::readEntry(const RegionDescriptor& readDesc,
                             RelAddress addr,
                             HashedKey expected,
                             Buffer& value) {
  // Because region opened for read, nobody will reclaim it or modify. Safe
  // without locks.
  auto permanent = regionManager_.getRegion(addr.rid()).isPinned();
  uint32_t size = 0;
  if (!permanent && allocator_.isSizeClassAllocator()) {
    size = regionManager_.getRegionSlotSize(addr.rid());
  } else {
    size = std::min(readBufferSize_, addr.offset());
  }

  XDCHECK_EQ(size % blockSize_, 0ULL)
      << folly::sformat("blockSize={}, size={}", blockSize_, size);

  auto buffer = regionManager_.makeIOBuffer(size);
  if (!regionManager_.read(readDesc, addr.sub(size), buffer.mutableView())) {
    return Status::DeviceError;
  }

  auto entryEnd = buffer.data() + buffer.size();
  auto desc = *reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
  if (desc.csSelf != desc.computeChecksum()) {
    lookupChecksumErrorCount_.inc();
    return Status::DeviceError;
  }

  BufferView key{desc.keySize, entryEnd - sizeof(EntryDesc) - desc.keySize};
  if (HashedKey::precomputed(key, desc.keyHash) != expected) {
    lookupFalsePositiveCount_.inc();
    return Status::NotFound;
  }

  if (permanent || !allocator_.isSizeClassAllocator()) {
    // Update slot size to actual, defined by key and value size
    size =
        allocator_.alignOnBlock(serializedSize(desc.keySize, desc.valueSize));
    if (buffer.size() > size) {
      // Read more than actual size. Do not re-read, but reallocate the buffer.
      buffer = Buffer{BufferView{desc.valueSize, entryEnd - size}};
    } else if (buffer.size() < size) {
      // Read less than actual size. Read again with proper buffer.
      buffer = regionManager_.makeIOBuffer(size);
      if (!regionManager_.read(
              readDesc, addr.sub(size), buffer.mutableView())) {
        return Status::DeviceError;
      }
    }
  }
  value = std::move(buffer);
  value.shrink(desc.valueSize);
  if (checksumData_ && desc.cs != checksum(value.view())) {
    value.reset();
    lookupChecksumErrorCount_.inc();
    return Status::DeviceError;
  }
  return Status::Ok;
}

void BlockCache::flush() {
  XLOG(INFO, "Flush block cache");
  allocator_.flush();
  regionManager_.flush();
}

void BlockCache::reset() {
  XLOG(INFO, "Reset block cache");
  index_.reset();
  // Allocator resets region manager
  allocator_.reset();

  // Reset counters
  insertCount_.set(0);
  lookupCount_.set(0);
  removeCount_.set(0);
  allocErrorCount_.set(0);
  logicalWrittenCount_.set(0);
  sizeDist_.reset();
  holeCount_.set(0);
  holeSizeTotal_.set(0);
}

void BlockCache::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_items", index_.computeSize());
  visitor("navy_bc_inserts", insertCount_.get());
  visitor("navy_bc_insert_hash_collisions", insertHashCollisionCount_.get());
  visitor("navy_bc_succ_inserts", succInsertCount_.get());
  visitor("navy_bc_lookups", lookupCount_.get());
  visitor("navy_bc_lookup_false_positives", lookupFalsePositiveCount_.get());
  visitor("navy_bc_lookup_checksum_errors", lookupChecksumErrorCount_.get());
  visitor("navy_bc_succ_lookups", succLookupCount_.get());
  visitor("navy_bc_removes", removeCount_.get());
  visitor("navy_bc_succ_removes", succRemoveCount_.get());
  visitor("navy_bc_eviction_lookup_misses", evictionLookupMissCounter_.get());
  visitor("navy_bc_alloc_errors", allocErrorCount_.get());
  visitor("navy_bc_logical_written", logicalWrittenCount_.get());
  visitor("navy_bc_hole_count", holeCount_.get());
  visitor("navy_bc_hole_bytes", holeSizeTotal_.get());
  visitor("navy_bc_reinsertions", reinsertionCount_.get());
  visitor("navy_bc_reinsertion_bytes", reinsertionBytes_.get());
  visitor("navy_bc_reinsertion_errors", reinsertionErrorCount_.get());

  auto snapshot = sizeDist_.getSnapshot();
  for (auto& kv : snapshot) {
    auto statName = folly::sformat("navy_bc_approx_bytes_in_size_{}", kv.first);
    visitor(statName.c_str(), kv.second);
  }

  // Allocator visits region manager
  allocator_.getCounters(visitor);

  if (reinsertionPolicy_) {
    reinsertionPolicy_->getCounters(visitor);
  }
}

void BlockCache::persist(RecordWriter& rw) {
  XLOG(INFO, "Starting block cache persist");
  auto config = config_;
  config.sizeDist = sizeDist_.getSnapshot();
  config.set_holeCount(holeCount_.get());
  config.set_holeSizeTotal(holeSizeTotal_.get());
  config.reinsertionPolicyEnabled = (reinsertionPolicy_ != nullptr);
  serializeProto(config, rw);
  regionManager_.persist(rw);
  index_.persist(rw);
  if (reinsertionPolicy_) {
    reinsertionPolicy_->persist(rw);
  }

  XLOG(INFO, "Finished block cache persist");
}

bool BlockCache::recover(RecordReader& rr) {
  XLOG(INFO, "Starting block cache recovery");
  reset();
  try {
    tryRecover(rr);
  } catch (const std::exception& e) {
    XLOGF(ERR, "Exception: {}", e.what());
    XLOG(ERR, "Failed to recover block cache. Resetting cache.");
    reset();
    return false;
  }
  XLOG(INFO, "Finished block cache recovery");
  return true;
}

void BlockCache::tryRecover(RecordReader& rr) {
  auto config = deserializeProto<serialization::BlockCacheConfig>(rr);
  if (!isValidRecoveryData(config)) {
    auto configStr = serializeToJson(config);
    XLOGF(ERR, "Recovery config: {}", configStr.c_str());
    throw std::invalid_argument("Recovery config does not match cache config");
  }
  sizeDist_ = SizeDistribution{config.sizeDist};
  holeCount_.set(config.holeCount);
  holeSizeTotal_.set(config.holeSizeTotal);
  regionManager_.recover(rr);
  index_.recover(rr);
  // Reinsertion policy is optional. So only try to recover if we had it
  // enabled in the last run of Navy.
  if (config.reinsertionPolicyEnabled && reinsertionPolicy_) {
    reinsertionPolicy_->recover(rr);
  }
}

bool BlockCache::isValidRecoveryData(
    const serialization::BlockCacheConfig& config) const {
  return config_.cacheBaseOffset == config.cacheBaseOffset &&
         config_.cacheSize == config.cacheSize &&
         config_.blockSize == config.blockSize &&
         config_.sizeClasses == config.sizeClasses &&
         config_.checksum == config.checksum &&
         config_.version == config.version;
}

serialization::BlockCacheConfig BlockCache::serializeConfig(
    const Config& config) {
  serialization::BlockCacheConfig serializedConfig;
  serializedConfig.cacheBaseOffset = config.cacheBaseOffset;
  serializedConfig.cacheSize = config.cacheSize;
  serializedConfig.blockSize = config.blockSize;
  serializedConfig.checksum = config.checksum;
  serializedConfig.version = kFormatVersion;
  for (auto sc : config.sizeClasses) {
    serializedConfig.sizeClasses.insert(sc);
  }
  return serializedConfig;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

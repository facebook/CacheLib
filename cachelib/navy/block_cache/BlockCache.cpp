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

#include "cachelib/navy/block_cache/BlockCache.h"

#include <folly/ScopeGuard.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstring>
#include <numeric>
#include <utility>

#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "folly/Range.h"

namespace facebook {
namespace cachelib {
namespace navy {

constexpr uint32_t BlockCache::kMinAllocAlignSize;
constexpr uint32_t BlockCache::kMaxItemSize;
constexpr uint32_t BlockCache::kFormatVersion;
constexpr uint32_t BlockCache::kDefReadBufferSize;
constexpr uint16_t BlockCache::kDefaultItemPriority;

BlockCache::Config& BlockCache::Config::validate() {
  XDCHECK_NE(scheduler, nullptr);
  if (!device || !evictionPolicy) {
    throw std::invalid_argument("missing required param");
  }
  if (regionSize > 256u << 20) {
    // We allocate region in memory to reclaim. Too large region will cause
    // problems: at least, long allocation times.
    throw std::invalid_argument("region is too large");
  }
  if (cacheSize <= 0) {
    throw std::invalid_argument("invalid size");
  }
  if (cacheSize % regionSize != 0) {
    throw std::invalid_argument(
        folly::sformat("Cache size is not aligned to region size! cache size: "
                       "{}, region size: {}",
                       cacheSize,
                       regionSize));
  }
  if (getNumRegions() < cleanRegionsPool) {
    throw std::invalid_argument("not enough space on device");
  }
  if (numInMemBuffers == 0) {
    throw std::invalid_argument("there must be at least one in-mem buffers");
  }
  if (numPriorities == 0) {
    throw std::invalid_argument("allocator must have at least one priority");
  }

  reinsertionConfig.validate();

  return *this;
}

void BlockCache::validate(BlockCache::Config& config) const {
  uint32_t allocAlignSize = calcAllocAlignSize();
  if (!folly::isPowTwo(allocAlignSize)) {
    throw std::invalid_argument("invalid block size");
  }
  if (config.regionSize % allocAlignSize != 0) {
    throw std::invalid_argument("invalid region size");
  }
  auto shiftWidth =
      facebook::cachelib::NumBits<decltype(RelAddress().offset())>::value;
  if (config.cacheSize > static_cast<uint64_t>(allocAlignSize) << shiftWidth) {
    throw std::invalid_argument(
        folly::sformat("can't address cache with {} bits", shiftWidth));
  }
}

uint32_t BlockCache::calcAllocAlignSize() const {
  // Shift the total device size by <RelAddressWidth-in-bits>,
  // to determine the size of the alloc alignment the device can support
  auto shiftWidth =
      facebook::cachelib::NumBits<decltype(RelAddress().offset())>::value;

  uint32_t allocAlignSize =
      static_cast<uint32_t>(device_.getSize() >> shiftWidth);
  if (allocAlignSize == 0 || allocAlignSize <= kMinAllocAlignSize) {
    return kMinAllocAlignSize;
  }
  if (folly::isPowTwo(allocAlignSize)) { // already power of 2
    return allocAlignSize;
  }

  // find the next 2-power value
  // The alloc align size must be 2-power value so that size classes do not
  // have to be modified whenever device size changes.
  return folly::nextPowTwo(allocAlignSize);
}

BlockCache::BlockCache(Config&& config)
    : BlockCache{std::move(config.validate()), ValidConfigTag{}} {}

BlockCache::BlockCache(Config&& config, ValidConfigTag)
    : config_{serializeConfig(config)},
      numPriorities_{config.numPriorities},
      checkExpired_{std::move(config.checkExpired)},
      destructorCb_{std::move(config.destructorCb)},
      checksumData_{config.checksum},
      device_{*config.device},
      allocAlignSize_{calcAllocAlignSize()},
      readBufferSize_{config.readBufferSize < kDefReadBufferSize
                          ? kDefReadBufferSize
                          : config.readBufferSize},
      regionSize_{config.regionSize},
      itemDestructorEnabled_{config.itemDestructorEnabled},
      preciseRemove_{config.preciseRemove},
      regionManager_{config.getNumRegions(),
                     config.regionSize,
                     config.cacheBaseOffset,
                     *config.device,
                     config.cleanRegionsPool,
                     *config.scheduler,
                     bindThis(&BlockCache::onRegionReclaim, *this),
                     bindThis(&BlockCache::onRegionCleanup, *this),
                     std::move(config.evictionPolicy),
                     config.numInMemBuffers,
                     config.numPriorities,
                     config.inMemBufFlushRetryLimit},
      allocator_{regionManager_, config.numPriorities},
      reinsertionPolicy_{makeReinsertionPolicy(config.reinsertionConfig)} {
  validate(config);
  XLOG(INFO, "Block cache created");
  XDCHECK_NE(readBufferSize_, 0u);
}
std::shared_ptr<BlockCacheReinsertionPolicy> BlockCache::makeReinsertionPolicy(
    const BlockCacheReinsertionConfig& reinsertionConfig) {
  auto hitsThreshold = reinsertionConfig.getHitsThreshold();
  if (hitsThreshold) {
    return std::make_shared<HitsReinsertionPolicy>(hitsThreshold, index_);
  }

  auto pctThreshold = reinsertionConfig.getPctThreshold();
  if (pctThreshold) {
    return std::make_shared<PercentageReinsertionPolicy>(pctThreshold);
  }
  return reinsertionConfig.getCustomPolicy();
}

uint32_t BlockCache::serializedSize(uint32_t keySize, uint32_t valueSize) {
  uint32_t size = sizeof(EntryDesc) + keySize + valueSize;
  return powTwoAlign(size, allocAlignSize_);
}

Status BlockCache::insert(HashedKey hk, BufferView value) {
  uint32_t size = serializedSize(hk.key().size(), value.size());
  if (size > kMaxItemSize) {
    allocErrorCount_.inc();
    insertCount_.inc();
    return Status::Rejected;
  }

  // All newly inserted items are assigned with the lowest priority
  auto [desc, slotSize, addr] = allocator_.allocate(size, kDefaultItemPriority);

  switch (desc.status()) {
  case OpenStatus::Error:
    allocErrorCount_.inc();
    insertCount_.inc();
    return Status::Rejected;
  case OpenStatus::Ready:
    insertCount_.inc();
    break;
  case OpenStatus::Retry:
    return Status::Retry;
  }
  // After allocation a region is opened for writing. Until we close it, the
  // region would not be reclaimed and index never gets an invalid entry.
  const auto status = writeEntry(addr, slotSize, hk, value);
  auto newObjSizeHint = encodeSizeHint(slotSize);
  if (status == Status::Ok) {
    const auto lr = index_.insert(
        hk.keyHash(), encodeRelAddress(addr.add(slotSize)), newObjSizeHint);
    // We replaced an existing key in the index
    uint64_t newObjSize = decodeSizeHint(newObjSizeHint);
    uint64_t oldObjSize = 0;
    if (lr.found()) {
      oldObjSize = decodeSizeHint(lr.sizeHint());
      holeSizeTotal_.add(oldObjSize);
      holeCount_.inc();
      insertHashCollisionCount_.inc();
    }
    succInsertCount_.inc();
    if (newObjSize < oldObjSize) {
      usedSizeBytes_.sub(oldObjSize - newObjSize);
    } else {
      usedSizeBytes_.add(newObjSize - oldObjSize);
    }
  }
  allocator_.close(std::move(desc));
  return status;
}

bool BlockCache::couldExist(HashedKey hk) {
  const auto lr = index_.lookup(hk.keyHash());
  if (!lr.found()) {
    lookupCount_.inc();
    return false;
  }
  return true;
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
  auto addrEnd = decodeRelAddress(lr.address());
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
    auto status =
        readEntry(desc, addrEnd, decodeSizeHint(lr.sizeHint()), hk, value);
    if (status == Status::Ok) {
      regionManager_.touch(addrEnd.rid());
      succLookupCount_.inc();
    }
    regionManager_.close(std::move(desc));
    lookupCount_.inc();
    return status;
  }
  case OpenStatus::Retry:
    return Status::Retry;
  default:
    // Open region never returns other statuses than above
    XDCHECK(false) << "unreachable";
    return Status::DeviceError;
  }
}

std::pair<Status, std::string> BlockCache::getRandomAlloc(Buffer& value) {
  // Get rendom region and offset within the region
  auto rid = regionManager_.getRandomRegion();
  auto& region = regionManager_.getRegion(rid);
  auto randOffset = folly::Random::rand32(0, regionSize_);
  auto offset = region.getLastEntryEndOffset();
  if (randOffset >= offset) {
    return std::make_pair(Status::NotFound, "");
  }

  const auto seqNumber = regionManager_.getSeqNumber();
  RegionDescriptor rdesc = regionManager_.openForRead(rid, seqNumber);
  if (rdesc.status() != OpenStatus::Ready) {
    return std::make_pair(Status::NotFound, "");
  }

  auto buffer = regionManager_.read(rdesc, RelAddress{rid, 0}, offset);
  // The region had been read out to Buffer, so can be closed here
  regionManager_.close(std::move(rdesc));
  if (buffer.size() != offset) {
    return std::make_pair(Status::NotFound, "");
  }

  // Iterate the entries backward until we find the entry
  // where the randOffset falls into
  while (offset > 0) {
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    if (desc.csSelf != desc.computeChecksum()) {
      XLOGF(ERR,
            "Item header checksum mismatch. Region {} is likely corrupted. "
            "Expected:{}, Actual: {}",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum());
      break;
    }

    RelAddress addrEnd{rid, offset};
    const auto entrySize = serializedSize(desc.keySize, desc.valueSize);

    XDCHECK_GE(offset, entrySize);
    offset -= entrySize; // start of this entry
    if (randOffset < offset) {
      continue;
    }

    BufferView valueView{desc.valueSize, entryEnd - entrySize};
    if (checksumData_ && desc.cs != checksum(valueView)) {
      XLOGF(ERR,
            "Item value checksum mismatch. Region {} is likely corrupted. "
            "Expected:{}, Actual: {}.",
            rid.index(),
            desc.cs,
            checksum(valueView));
      break;
    }

    // confirm that the chosen NvmItem is still being mapped with the key
    HashedKey hk =
        makeHK(entryEnd - sizeof(EntryDesc) - desc.keySize, desc.keySize);
    const auto lr = index_.lookup(hk.keyHash());
    if (!lr.found() || addrEnd != decodeRelAddress(lr.address())) {
      // overwritten
      break;
    }

    // The entry is within the region buffer, so copy it out to new Buffer
    value = Buffer(valueView);
    return std::make_pair(Status::Ok, hk.key().str());
  }

  return std::make_pair(Status::NotFound, "");
}

Status BlockCache::remove(HashedKey hk) {
  removeCount_.inc();

  Buffer value;
  if ((itemDestructorEnabled_ && destructorCb_) || preciseRemove_) {
    Status status = lookup(hk, value);

    if (status != Status::Ok) {
      // device error, or region reclaimed, or item not found
      value.reset();
      if (status == Status::Retry) {
        return status;
      } else if (status != Status::NotFound) {
        lookupForItemDestructorErrorCount_.inc();
        // still fail after retry, return a BadState to disable navy
        return Status::BadState;
      } else {
        // NotFound
        removeAttemptCollisions_.inc();
        if (preciseRemove_) {
          return status;
        }
      }
    }
  }

  auto lr = index_.remove(hk.keyHash());
  if (lr.found()) {
    uint64_t removedObjectSize = decodeSizeHint(lr.sizeHint());
    holeSizeTotal_.add(removedObjectSize);
    holeCount_.inc();
    usedSizeBytes_.sub(removedObjectSize);
    succRemoveCount_.inc();
    if (!value.isNull() && destructorCb_) {
      destructorCb_(hk, value.view(), DestructorEvent::Removed);
    }
    return Status::Ok;
  }
  return Status::NotFound;
}

// Remove all region entries from index and invoke callback
// See @RegionEvictCallback for details
uint32_t BlockCache::onRegionReclaim(RegionId rid, BufferView buffer) {
  // Eviction callback guarantees are the following:
  //   - Every value inserted will get eviction callback at some point of
  //     time.
  //   - It is invoked only once per insertion.
  //
  // We do not guarantee time between remove and callback invocation. If a
  // value v1 was replaced with v2 user will get callbacks for both v1 and
  // v2 when they are evicted (in no particular order).
  uint32_t evictionCount = 0; // item that was evicted during reclaim
  auto& region = regionManager_.getRegion(rid);
  auto offset = region.getLastEntryEndOffset();
  while (offset > 0) {
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    if (desc.csSelf != desc.computeChecksum()) {
      reclaimEntryHeaderChecksumErrorCount_.inc();
      XLOGF(ERR,
            "Item header checksum mismatch. Region {} is likely corrupted. "
            "Expected:{}, Actual: {}. Aborting reclaim. Remaining items in the "
            "region will not be cleaned up (destructor won't be invoked).",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum());
      break;
    }

    const auto entrySize = serializedSize(desc.keySize, desc.valueSize);
    HashedKey hk =
        makeHK(entryEnd - sizeof(EntryDesc) - desc.keySize, desc.keySize);
    BufferView value{desc.valueSize, entryEnd - entrySize};
    if (checksumData_ && desc.cs != checksum(value)) {
      // We do not need to abort here since the EntryDesc checksum was good, so
      // we can safely proceed to read the next entry.
      reclaimValueChecksumErrorCount_.inc();
    }

    const auto reinsertionRes =
        reinsertOrRemoveItem(hk, value, entrySize, RelAddress{rid, offset});
    switch (reinsertionRes) {
    case ReinsertionRes::kEvicted:
      evictionCount++;
      usedSizeBytes_.sub(decodeSizeHint(encodeSizeHint(entrySize)));
      break;
    case ReinsertionRes::kRemoved:
      holeCount_.sub(1);
      holeSizeTotal_.sub(decodeSizeHint(encodeSizeHint(entrySize)));
      break;
    case ReinsertionRes::kReinserted:
      break;
    }

    if (destructorCb_ && reinsertionRes == ReinsertionRes::kEvicted) {
      destructorCb_(hk, value, DestructorEvent::Recycled);
    }
    XDCHECK_GE(offset, entrySize);
    offset -= entrySize;
  }

  XDCHECK_GE(region.getNumItems(), evictionCount);
  return evictionCount;
}

void BlockCache::onRegionCleanup(RegionId rid, BufferView buffer) {
  uint32_t evictionCount = 0; // item that was evicted during cleanup
  auto& region = regionManager_.getRegion(rid);
  auto offset = region.getLastEntryEndOffset();
  while (offset > 0) {
    // iterate each entry
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    if (desc.csSelf != desc.computeChecksum()) {
      cleanupEntryHeaderChecksumErrorCount_.inc();
      XLOGF(ERR,
            "Item header checksum mismatch. Region {} is likely corrupted. "
            "Expected:{}, Actual: {}",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum());
      break;
    }

    const auto entrySize = serializedSize(desc.keySize, desc.valueSize);
    HashedKey hk =
        makeHK(entryEnd - sizeof(EntryDesc) - desc.keySize, desc.keySize);
    BufferView value{desc.valueSize, entryEnd - entrySize};
    if (checksumData_ && desc.cs != checksum(value)) {
      // We do not need to abort here since the EntryDesc checksum was good, so
      // we can safely proceed to read the next entry.
      cleanupValueChecksumErrorCount_.inc();
    }

    // remove the item
    auto removeRes = removeItem(hk, RelAddress{rid, offset});
    if (removeRes) {
      evictionCount++;
      usedSizeBytes_.sub(decodeSizeHint(encodeSizeHint(entrySize)));
    } else {
      holeCount_.sub(1);
      holeSizeTotal_.sub(decodeSizeHint(encodeSizeHint(entrySize)));
    }
    if (destructorCb_ && removeRes) {
      destructorCb_(hk, value, DestructorEvent::Recycled);
    }
    XDCHECK_GE(offset, entrySize);
    offset -= entrySize;
  }

  XDCHECK_GE(region.getNumItems(), evictionCount);
}

bool BlockCache::removeItem(HashedKey hk, RelAddress currAddr) {
  if (index_.removeIfMatch(hk.keyHash(), encodeRelAddress(currAddr))) {
    return true;
  }
  evictionLookupMissCounter_.inc();
  return false;
}

BlockCache::ReinsertionRes BlockCache::reinsertOrRemoveItem(
    HashedKey hk, BufferView value, uint32_t entrySize, RelAddress currAddr) {
  auto removeItem = [this, hk, currAddr](bool expired) {
    if (index_.removeIfMatch(hk.keyHash(), encodeRelAddress(currAddr))) {
      if (expired) {
        evictionExpiredCount_.inc();
      }
      return ReinsertionRes::kEvicted;
    }
    return ReinsertionRes::kRemoved;
  };

  const auto lr = index_.peek(hk.keyHash());
  if (!lr.found() || decodeRelAddress(lr.address()) != currAddr) {
    evictionLookupMissCounter_.inc();
    return ReinsertionRes::kRemoved;
  }

  if (checkExpired_ && checkExpired_(value)) {
    return removeItem(true);
  }

  if (!reinsertionPolicy_ || !reinsertionPolicy_->shouldReinsert(hk.key())) {
    return removeItem(false);
  }

  // Priority of an re-inserted item is determined by its past accesses
  // since the time it was last (re)inserted.
  // TODO: T95784621 this should be made configurable by having the reinsertion
  //       policy return a priority rank for an item.
  uint16_t priority =
      numPriorities_ == 0
          ? kDefaultItemPriority
          : std::min<uint16_t>(lr.currentHits(), numPriorities_ - 1);

  uint32_t size = serializedSize(hk.key().size(), value.size());
  auto [desc, slotSize, addr] = allocator_.allocate(size, priority);

  switch (desc.status()) {
  case OpenStatus::Ready:
    break;
  case OpenStatus::Error:
    allocErrorCount_.inc();
    reinsertionErrorCount_.inc();
    break;
  case OpenStatus::Retry:
    reinsertionErrorCount_.inc();
    return removeItem(false);
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
    return removeItem(false);
  }

  const auto replaced =
      index_.replaceIfMatch(hk.keyHash(),
                            encodeRelAddress(addr.add(slotSize)),
                            encodeRelAddress(currAddr));
  if (!replaced) {
    reinsertionErrorCount_.inc();
    return removeItem(false);
  }
  reinsertionCount_.inc();
  reinsertionBytes_.add(entrySize);
  return ReinsertionRes::kReinserted;
}

Status BlockCache::writeEntry(RelAddress addr,
                              uint32_t slotSize,
                              HashedKey hk,
                              BufferView value) {
  XDCHECK_LE(addr.offset() + slotSize, regionManager_.regionSize());
  XDCHECK_EQ(slotSize % allocAlignSize_, 0ULL)
      << folly::sformat(" alignSize={}, size={}", allocAlignSize_, slotSize);
  auto buffer = Buffer(slotSize);

  // Copy descriptor and the key to the end
  size_t descOffset = buffer.size() - sizeof(EntryDesc);
  auto desc = new (buffer.data() + descOffset)
      EntryDesc(hk.key().size(), value.size(), hk.keyHash());
  if (checksumData_) {
    desc->cs = checksum(value);
  }

  buffer.copyFrom(descOffset - hk.key().size(), makeView(hk.key()));
  buffer.copyFrom(0, value);

  regionManager_.write(addr, std::move(buffer));
  logicalWrittenCount_.add(hk.key().size() + value.size());
  return Status::Ok;
}

Status BlockCache::readEntry(const RegionDescriptor& readDesc,
                             RelAddress addr,
                             uint32_t approxSize,
                             HashedKey expected,
                             Buffer& value) {
  // Because region opened for read, nobody will reclaim it or modify. Safe
  // without locks.

  // The item layout is as thus
  // | --- value --- | --- empty --- | --- header --- |
  // The size itself is determined by serializedSize(), so
  // we will try to read exactly that size or just slightly over.

  // Because we either use a predefined read buffer size, or align the size
  // up by kMinAllocAlignSize, our size might be bigger than the actual item
  // size. So we need to ensure we're not reading past the region's beginning.
  approxSize = std::min(approxSize, addr.offset());

  XDCHECK_EQ(approxSize % allocAlignSize_, 0ULL) << folly::sformat(
      " alignSize={}, approxSize={}", allocAlignSize_, approxSize);

  // Because we are going to look for EntryDesc in the buffer read, the buffer
  // must be atleast as big as EntryDesc aligned to next 2 power
  XDCHECK_GE(approxSize, folly::nextPowTwo(sizeof(EntryDesc)));

  auto buffer = regionManager_.read(readDesc, addr.sub(approxSize), approxSize);
  if (buffer.isNull()) {
    return Status::DeviceError;
  }

  auto entryEnd = buffer.data() + buffer.size();
  auto desc = *reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
  if (desc.csSelf != desc.computeChecksum()) {
    lookupEntryHeaderChecksumErrorCount_.inc();
    return Status::DeviceError;
  }

  folly::StringPiece key{reinterpret_cast<const char*>(
                             entryEnd - sizeof(EntryDesc) - desc.keySize),
                         desc.keySize};
  if (HashedKey::precomputed(key, desc.keyHash) != expected) {
    lookupFalsePositiveCount_.inc();
    return Status::NotFound;
  }

  // Update slot size to actual, defined by key and value size
  uint32_t size = serializedSize(desc.keySize, desc.valueSize);
  if (buffer.size() > size) {
    // Read more than actual size. Trim the invalid data in the beginning
    buffer.trimStart(buffer.size() - size);
  } else if (buffer.size() < size) {
    // Read less than actual size. Read again with proper buffer.
    buffer = regionManager_.read(readDesc, addr.sub(size), size);
    if (buffer.isNull()) {
      return Status::DeviceError;
    }
  }

  value = std::move(buffer);
  value.shrink(desc.valueSize);
  if (checksumData_ && desc.cs != checksum(value.view())) {
    XLOG_N_PER_MS(ERR, 10, 10'000) << folly::sformat(
        "Item value checksum mismatch when looking up key {}. "
        "Expected:{}, Actual: {}.",
        key, desc.cs, checksum(value.view()));
    value.reset();
    lookupValueChecksumErrorCount_.inc();
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
  holeCount_.set(0);
  holeSizeTotal_.set(0);
  usedSizeBytes_.set(0);
}

void BlockCache::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_size", getSize());
  visitor("navy_bc_items", index_.computeSize());
  visitor("navy_bc_inserts", insertCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_insert_hash_collisions", insertHashCollisionCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_succ_inserts", succInsertCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookups", lookupCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookup_false_positives", lookupFalsePositiveCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookup_entry_header_checksum_errors",
          lookupEntryHeaderChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookup_value_checksum_errors",
          lookupValueChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_reclaim_entry_header_checksum_errors",
          reclaimEntryHeaderChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_reclaim_value_checksum_errors",
          reclaimValueChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_cleanup_entry_header_checksum_errors",
          cleanupEntryHeaderChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_cleanup_value_checksum_errors",
          cleanupValueChecksumErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_succ_lookups", succLookupCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_removes", removeCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_succ_removes", succRemoveCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_eviction_lookup_misses", evictionLookupMissCounter_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_evictions_expired", evictionExpiredCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_alloc_errors", allocErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_logical_written", logicalWrittenCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_hole_count", holeCount_.get());
  visitor("navy_bc_hole_bytes", holeSizeTotal_.get());
  visitor("navy_bc_used_size_bytes", usedSizeBytes_.get());
  visitor("navy_bc_reinsertions", reinsertionCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_reinsertion_bytes", reinsertionBytes_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_reinsertion_errors", reinsertionErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookup_for_item_destructor_errors",
          lookupForItemDestructorErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_remove_attempt_collisions", removeAttemptCollisions_.get(),
          CounterVisitor::CounterType::RATE);
  // Allocator visits region manager
  allocator_.getCounters(visitor);
  index_.getCounters(visitor);

  if (reinsertionPolicy_) {
    reinsertionPolicy_->getCounters(visitor);
  }
}

void BlockCache::persist(RecordWriter& rw) {
  XLOG(INFO, "Starting block cache persist");
  auto config = config_;
  *config.allocAlignSize() = allocAlignSize_;
  config.holeCount() = holeCount_.get();
  config.holeSizeTotal() = holeSizeTotal_.get();
  *config.usedSizeBytes() = usedSizeBytes_.get();
  *config.reinsertionPolicyEnabled() = (reinsertionPolicy_ != nullptr);
  serializeProto(config, rw);
  regionManager_.persist(rw);
  index_.persist(rw);

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
  holeCount_.set(*config.holeCount());
  holeSizeTotal_.set(*config.holeSizeTotal());
  usedSizeBytes_.set(*config.usedSizeBytes());
  regionManager_.recover(rr);
  index_.recover(rr);
}

bool BlockCache::isValidRecoveryData(
    const serialization::BlockCacheConfig& recoveredConfig) const {
  return *config_.cacheBaseOffset_ref() ==
             *recoveredConfig.cacheBaseOffset_ref() &&
         *config_.cacheSize_ref() == *recoveredConfig.cacheSize_ref() &&
         static_cast<int32_t>(allocAlignSize_) ==
             *recoveredConfig.allocAlignSize_ref() &&
         *config_.checksum_ref() == *recoveredConfig.checksum_ref() &&
         *config_.version_ref() == *recoveredConfig.version_ref();
}

serialization::BlockCacheConfig BlockCache::serializeConfig(
    const Config& config) {
  serialization::BlockCacheConfig serializedConfig;
  *serializedConfig.cacheBaseOffset() = config.cacheBaseOffset;
  *serializedConfig.cacheSize() = config.cacheSize;
  *serializedConfig.checksum() = config.checksum;
  *serializedConfig.version() = kFormatVersion;
  return serializedConfig;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

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
#include <utility>

#include "cachelib/common/Time.h"
#include "cachelib/common/inject_pause.h"
#include "cachelib/navy/block_cache/FixedSizeIndex.h"
#include "cachelib/navy/block_cache/HitsReinsertionPolicy.h"
#include "cachelib/navy/block_cache/PercentageReinsertionPolicy.h"
#include "cachelib/navy/block_cache/SparseMapIndex.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "folly/Range.h"

namespace facebook::cachelib::navy {

BlockCache::Config& BlockCache::Config::validate() {
  if (!device || !evictionPolicy) {
    throw std::invalid_argument("missing required param");
  }
  if (regionSize > 1024u << 20) {
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
  if (allocatorsPerPriority.size() == 0) {
    throw std::invalid_argument("Allocators must have at least one priority");
  }
  for (const auto& ac : allocatorsPerPriority) {
    if (ac == 0) {
      throw std::invalid_argument(
          "Each priority must have at least one allocator");
    }
  }

  reinsertionConfig.validate();
  indexConfig.validate();

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

std::unique_ptr<Index> BlockCache::createIndex(
    const BlockCacheIndexConfig& indexConfig,
    const NavyPersistParams& persistParams,
    const std::string& name) {
  if (indexConfig.isFixedSizeIndexEnabled()) {
    return std::make_unique<FixedSizeIndex>(
        indexConfig.getNumChunks(),
        indexConfig.getNumBucketsPerChunkPower(),
        indexConfig.getNumBucketsPerMutex(),
        (indexConfig.useShmToPersist() || persistParams.useShm)
            ? (persistParams.shmManager.has_value()
                   ? &(persistParams.shmManager.value().get())
                   : nullptr)
            : nullptr,
        name);
  } else {
    return std::make_unique<SparseMapIndex>(
        indexConfig.getNumSparseMapBuckets(),
        indexConfig.getNumBucketsPerMutex(),
        indexConfig.isTrackItemHistoryEnabled()
            ? SparseMapIndex::ExtraField::kItemHitHistory
            : SparseMapIndex::ExtraField::kTotalHits);
  }
}

BlockCache::BlockCache(Config&& config)
    : BlockCache{std::move(config.validate()), ValidConfigTag{}} {}

BlockCache::BlockCache(Config&& config, ValidConfigTag)
    : config_{serializeConfig(config)},
      numPriorities_{
          static_cast<uint16_t>(config.allocatorsPerPriority.size())},
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
      index_(
          createIndex(config.indexConfig, config.persistParams, config.name)),
      regionManager_{config.getNumRegions(),
                     config.regionSize,
                     config.cacheBaseOffset,
                     *config.device,
                     config.cleanRegionsPool,
                     config.cleanRegionThreads,
                     config.stackSize,
                     bindThis(&BlockCache::onRegionReclaim, *this),
                     bindThis(&BlockCache::onRegionCleanup, *this),
                     std::move(config.evictionPolicy),
                     config.numInMemBuffers,
                     static_cast<uint16_t>(config.allocatorsPerPriority.size()),
                     config.inMemBufFlushRetryLimit,
                     config.regionManagerFlushAsync,
                     true /* allowReadDuringReclaim */},
      allocator_{regionManager_, config.allocatorsPerPriority},
      reinsertionPolicy_{makeReinsertionPolicy(config.reinsertionConfig)} {
  validate(config);
  XLOG(INFO, "Block cache created");
  XDCHECK_NE(readBufferSize_, 0u);

  legacyEventTracker_ = config.legacyEventTracker;
}

std::shared_ptr<BlockCacheReinsertionPolicy> BlockCache::makeReinsertionPolicy(
    const BlockCacheReinsertionConfig& reinsertionConfig) {
  auto hitsThreshold = reinsertionConfig.getHitsThreshold();
  if (hitsThreshold) {
    return std::make_shared<HitsReinsertionPolicy>(hitsThreshold, *index_);
  }

  auto pctThreshold = reinsertionConfig.getPctThreshold();
  if (pctThreshold) {
    return std::make_shared<PercentageReinsertionPolicy>(pctThreshold);
  }
  return reinsertionConfig.getCustomPolicy(*index_);
}

uint32_t BlockCache::serializedSize(uint32_t keySize,
                                    uint32_t valueSize) const {
  uint32_t size = sizeof(EntryDesc) + keySize + valueSize;
  return powTwoAlign(size, allocAlignSize_);
}

Status BlockCache::insert(HashedKey hk, BufferView value) {
  auto start = getSteadyClock();
  SCOPE_EXIT {
    insertLatency_.trackValue(toMicros(getSteadyClock() - start).count());
  };
  INJECT_PAUSE(pause_blockcache_insert_entry);

  auto allocateRes = allocateForInsert(hk, value.size());
  if (allocateRes.hasError()) {
    return allocateRes.error();
  }
  auto& [desc, slotSize, addr] = allocateRes.value();

  // After allocation a region is opened for writing. Until we close it, the
  // region would not be reclaimed and index never gets an invalid entry.
  writeEntry(addr, slotSize, hk, value);
  updateIndex(hk.keyHash(), slotSize, addr, /* allowReplace */ true);
  allocator_.close(std::move(desc));
  INJECT_PAUSE(pause_blockcache_insert_done);
  return Status::Ok;
}

bool BlockCache::updateIndex(uint64_t keyHash,
                             uint32_t size,
                             const RelAddress& addr,
                             bool allowReplace) const {
  auto newObjSizeHint = encodeSizeHint(size);
  auto encodedAddr = encodeRelAddress(addr.add(size));
  const auto lr =
      allowReplace
          ? index_->insert(keyHash, encodedAddr, newObjSizeHint)
          : index_->insertIfNotExists(keyHash, encodedAddr, newObjSizeHint);
  // We replaced an existing key in the index
  uint64_t newObjSize = decodeSizeHint(newObjSizeHint);
  uint64_t oldObjSize = 0;
  if (lr.found()) {
    insertHashCollisionCount_.inc();
    if (allowReplace) {
      oldObjSize = decodeSizeHint(lr.sizeHint());
      addHole(oldObjSize);
    } else {
      return false;
    }
  }
  succInsertCount_.inc();
  if (newObjSize < oldObjSize) {
    usedSizeBytes_.sub(oldObjSize - newObjSize);
  } else {
    usedSizeBytes_.add(newObjSize - oldObjSize);
  }
  return true;
}

bool BlockCache::couldExist(HashedKey hk) {
  const auto lr = index_->peek(hk.keyHash());
  if (!lr.found()) {
    lookupCount_.inc();
    return false;
  }
  return true;
}

uint64_t BlockCache::estimateWriteSize(HashedKey hk, BufferView value) const {
  return serializedSize(hk.key().size(), value.size());
}

BlockCache::LookupData BlockCache::lookupInternal(HashedKey hk) {
  auto start = getSteadyClock();
  SCOPE_EXIT {
    lookupLatency_.trackValue(toMicros(getSteadyClock() - start).count());
  };

  auto retryIndex = (regionManager_.readAllowedDuringReclaim() ? 1 : 0);
  Index::LookupResult lr;
  RelAddress addrEnd;
  LookupData ld;

  do {
    auto seqNumber = regionManager_.getSeqNumber();
    lr = index_->lookup(hk.keyHash());
    if (!lr.found()) {
      lookupCount_.inc();
      ld.status_ = Status::NotFound;
      return ld;
    }
    // If relative address has offset 0, the entry actually belongs to the
    // previous region (this is address of its end). To compensate for this, we
    // subtract 1 before conversion and add after to relative address.
    addrEnd = decodeRelAddress(lr.address());
    // Between acquiring @seqNumber and @openForRead, reclaim may start. There
    // are two options what can happen in @openForRead:
    //  - Reclaim in progress and open will fail because access mask
    //  disables both read and write (Only when read is not allowed during
    //  reclaim)
    //  - Reclaim finished and reclaimed victim was released/reset. Sequence
    //  number increased with that and open will fail because of sequence number
    //  check. (This means sequence number will be increased when we finish
    //  reclaim.)
    ld.desc_ = regionManager_.openForRead(addrEnd.rid(), seqNumber);
    // If we get OpenStatus::Retry when read is allowed during reclaim, it means
    // reclaim has finished and reclaimed victim was reset/released (checked by
    // SeqNumber). Index should had been updated in this case (to the new
    // location or as removed). So we need to lookup index again.
    //
    // In case read is NOT allowed during reclaim, there's no chance that this
    // situation is resolved by looking up the index again. So we'll follow the
    // old logic (just returning retry below)
  } while (ld.desc_.status() == OpenStatus::Retry && retryIndex-- > 0);

  switch (ld.desc_.status()) {
  case OpenStatus::Ready: {
    ld.status_ = readEntry(ld, addrEnd, decodeSizeHint(lr.sizeHint()), hk);

    if (FOLLY_UNLIKELY(ld.status_ == Status::DeviceError)) {
      // Remove this item from index so no future lookup will
      // ever attempt to read this key. Reclaim will also not be
      // able to re-insert this item as it does not exist in index.
      index_->remove(hk.keyHash());
    } else if (ld.status_ == Status::ChecksumError) {
      // In case we are getting transient checksum error, we will retry to read
      // the entry (S421120)
      ld.status_ = readEntry(ld, addrEnd, decodeSizeHint(lr.sizeHint()), hk);
      XLOGF(ERR,
            "Retry reading an entry after checksum error. Return code: "
            "{}",
            ld.status_);
      retryReadCount_.inc();

      if (ld.status_ != Status::Ok) {
        // Still failing. Remove this item from index so no future lookup will
        // ever attempt to read this key. Reclaim will also not be
        // able to re-insert this item as it does not exist in index.
        index_->remove(hk.keyHash());
      }
    }

    if (ld.status_ == Status::Ok) {
      regionManager_.touch(addrEnd.rid());
      succLookupCount_.inc();
    }
    lookupCount_.inc();
    if (reinsertionPolicy_) {
      reinsertionPolicy_->onLookup(hk.key());
    }
    break;
  }
  case OpenStatus::Retry:
    ld.status_ = Status::Retry;
    break;
  default:
    // Open region never returns statuses other than what's above
    XDCHECK(false) << "unreachable";
    ld.status_ = Status::DeviceError;
    break;
  }
  return ld;
}

Status BlockCache::lookup(HashedKey hk, Buffer& value) {
  auto ld = lookupInternal(hk);
  if (ld.desc_.isReady()) {
    regionManager_.close(std::move(ld.desc_));
  }
  if (ld.status_ == Status::Ok) {
    value = std::move(ld.buffer_);
    value.shrink(ld.valueSize_);
  }
  return ld.status_;
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
    RelAddress addrEnd{rid, offset};
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    if (desc.csSelf != desc.computeChecksum()) {
      XLOGF(ERR,
            "Item header checksum mismatch in getRandomAlloc(). Region {} is "
            "likely corrupted. Expected: {}, Actual: {}, Offset-end: {}, "
            "Physical-offset-end: {}, Header size: {}, Header (hex): {}",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum(),
            addrEnd.offset(),
            regionManager_.physicalOffset(addrEnd),
            sizeof(EntryDesc),
            folly::hexlify(
                folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
      break;
    }

    const auto entrySize = serializedSize(desc.keySize, desc.valueSize);

    XDCHECK_GE(offset, entrySize);
    offset -= entrySize; // start of this entry
    if (randOffset < offset) {
      continue;
    }

    BufferView valueView{desc.valueSize, entryEnd - entrySize};
    if (checksumData_ && desc.cs != checksum(valueView)) {
      XLOGF(ERR,
            "Item value checksum mismatch in getRandomAlloc(). Region {} is "
            "likely corrupted. Expected: {}, Actual: {}, Offset: {}, "
            "Physical-offset: {}, Value-size: {}, Payload (hex): {}",
            rid.index(),
            desc.cs,
            checksum(valueView),
            addrEnd.offset() - entrySize,
            regionManager_.physicalOffset(addrEnd) - entrySize,
            desc.valueSize,
            // call folly::unhexlify to convert it back to binary data
            folly::hexlify(
                folly::ByteRange(valueView.data(), valueView.dataEnd())));
      break;
    }

    // confirm that the chosen NvmItem is still being mapped with the key
    HashedKey hk =
        makeHK(entryEnd - sizeof(EntryDesc) - desc.keySize, desc.keySize);
    const auto lr = index_->peek(hk.keyHash());
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

Status BlockCache::removeImpl(const HashedKey& hk, const Buffer& value) {
  auto lr = index_->remove(hk.keyHash());
  if (lr.found()) {
    uint64_t removedObjectSize = decodeSizeHint(lr.sizeHint());
    addHole(removedObjectSize);
    usedSizeBytes_.sub(removedObjectSize);
    succRemoveCount_.inc();
    if (!value.isNull() && destructorCb_) {
      destructorCb_(hk, value.view(), DestructorEvent::Removed);
    }
    return Status::Ok;
  }
  return Status::NotFound;
}

Status BlockCache::remove(HashedKey hk) {
  auto start = getSteadyClock();
  SCOPE_EXIT {
    removeLatency_.trackValue(toMicros(getSteadyClock() - start).count());
  };
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
        if (status == Status::ChecksumError) {
          // checksum error doesn't need to be treated as a whole device's bad
          // status
          return status;
        } else {
          // still fail after retry, return a BadState to disable navy
          return Status::BadState;
        }
      } else {
        // NotFound
        removeAttemptCollisions_.inc();
        if (preciseRemove_) {
          return status;
        }
      }
    }
  }

  return removeImpl(hk, value);
}

/**
 * Callback for region eviction.
 *
 * This function is called when a region in the block cache is being evicted.
 * The evicted region is reclaimed by iterating over each item in the region and
 * either removing, evicting, or reinserting the item, depending on its state
 * and policy. This process frees up the region for reuse.
 *
 * If a checksum error is detected in an item's descriptor, the reclaim process
 * is aborted for the remaining items in the region, and their eviction
 * callbacks will not be invoked. If a checksum error is detected in the value,
 * we can continue working on the remaining items as the metadata is still valid
 * and that is what we need to find the next item to evict/remove/reinsert.
 *
 * Note: The time between item removal and callback invocation is not
 * guaranteed. If an item is replaced, callback is skipped and called when the
 * new item is evicted.
 *
 * @param rid    The RegionId of the region being evicted.
 * @param buffer A BufferView containing the data of the region to be reclaimed.
 * @return       The number of items successfully evicted from the region.
 *
 * See @RegionEvictCallback for further details.
 */
uint32_t BlockCache::onRegionReclaim(RegionId rid, BufferView buffer) {
  uint32_t evictionCount = 0;
  auto& region = regionManager_.getRegion(rid);
  auto offset = region.getLastEntryEndOffset();
  while (offset > 0) {
    RelAddress addrEnd(rid, offset);
    auto entryEnd = buffer.data() + offset;
    auto desc =
        *reinterpret_cast<const EntryDesc*>(entryEnd - sizeof(EntryDesc));
    if (desc.csSelf != desc.computeChecksum()) {
      reclaimEntryHeaderChecksumErrorCount_.inc();
      XLOGF(ERR,
            "Item header checksum mismatch in onRegionReclaim(). Item {} is "
            "likely corrupted. Aborting reclaim. This and remaining items in "
            "the region "
            "will not be cleaned up (destructor won't be invoked) nor "
            "reinserted. ",
            "Expected: {}, Actual: {}, Offset-end: {}, Physical-offset-end: {},"
            " Header size: {}, Header (hex): {}",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum(),
            addrEnd.offset(),
            regionManager_.physicalOffset(addrEnd),
            sizeof(EntryDesc),
            folly::hexlify(
                folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
      break;
    }
    /*
     * Entry Layout:
     * | Value | Padding | Key | EntryDesc |
     * ^                                 ^
     * entryEnd - entrySize            entryEnd
     * (value starts here)
     */
    const auto entrySize = serializedSize(desc.keySize, desc.valueSize);
    HashedKey hk =
        makeHK(entryEnd - sizeof(EntryDesc) - desc.keySize, desc.keySize);
    BufferView value{desc.valueSize, entryEnd - entrySize};

    AllocatorApiResult reinsertionRes = reinsertOrRemoveItem(
        hk, value, entrySize, RelAddress{rid, offset}, desc);
    switch (reinsertionRes) {
    case AllocatorApiResult::EVICTED:
    case AllocatorApiResult::EXPIRED:
      evictionCount++;
      usedSizeBytes_.sub(decodeSizeHint(encodeSizeHint(entrySize)));
      break;
    case AllocatorApiResult::REMOVED:
      removeHole(decodeSizeHint(encodeSizeHint(entrySize)));
      break;
    default:
      break;
    }
    if (destructorCb_ && (reinsertionRes == AllocatorApiResult::EVICTED ||
                          reinsertionRes == AllocatorApiResult::EXPIRED)) {
      destructorCb_(hk, value, DestructorEvent::Recycled);
    } else {
      recordEvent(hk.key(), AllocatorApiEvent::NVM_EVICT, reinsertionRes,
                  entrySize);
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
            "Item header checksum mismatch in onRegionCleanup(). Region {} is "
            "likely corrupted. Expected: {}, Actual: {}, Offset-end: {}, "
            "Physical-offset-end: {}, Header size: {}, Header (hex): {}",
            rid.index(),
            desc.csSelf,
            desc.computeChecksum(),
            offset,
            regionManager_.physicalOffset(RelAddress{rid, offset}),
            sizeof(EntryDesc),
            folly::hexlify(
                folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
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
      removeHole(decodeSizeHint(encodeSizeHint(entrySize)));
    }
    if (destructorCb_ && removeRes) {
      destructorCb_(hk, value, DestructorEvent::Recycled);
    }
    XDCHECK_GE(offset, entrySize);
    offset -= entrySize;
  }

  XDCHECK_GE(region.getNumItems(), evictionCount);
}

// To retrieve the 64bit key hash from the given address.
// This function will not (and cannot) check if index changed in between this
// function call for whatever reasons. It's caller's responsibility to make sure
// the index is not changed and no race condition happens.
//
std::optional<uint64_t> BlockCache::onKeyHashRetrievalFromLocation(
    uint32_t address) {
  auto addrEnd = decodeRelAddress(address);
  // It won't care about seq number here (giving it as std::nullopt). It's
  // caller's responsibility to check about the race condition for the index
  // change.
  auto desc = regionManager_.openForRead(addrEnd.rid(), std::nullopt);

  if (desc.status() != OpenStatus::Ready) {
    // In rare cases, Region can return 'Retry' when it's being released after
    // the reclaim. Checking the index again will resolve it since index should
    // have been updated with the new location
    return std::nullopt;
  }

  EntryDesc entryDesc{};
  uint8_t* entryEnd{nullptr};
  {
    SCOPE_EXIT { regionManager_.close(std::move(desc)); };
    // We only need to read EntryDesc + key. (Key is just for integrity check).
    // Since we don't know the key size before reading EntryDesc, let's just
    // read enough buffer to cover most keys. If it's not enough, we can read
    // more for the key, but that'll be rare cases.
    auto readSize = std::min(512u, addrEnd.offset());

    auto buffer = regionManager_.read(desc, addrEnd.sub(readSize), readSize);
    if (buffer.isNull()) {
      return std::nullopt;
    }

    entryEnd = buffer.data() + buffer.size();
    entryDesc = *reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
    // check if we have enough buffer read to cover the key
    if (buffer.size() < sizeof(EntryDesc) + entryDesc.keySize) {
      // re-read to cover the key. This should be just for the larger keys
      readSize = sizeof(EntryDesc) + entryDesc.keySize;
      if (readSize > addrEnd.offset()) {
        // something's wrong with the key size. It's not within the region
        // boundary
        XLOGF(
            ERR,
            "Key size {} in item header is crossing the region boundary in "
            "onKeyHashRetrievalFromLocation(). Region {} is likely corrupted. "
            "Offset-end: {}, Physical-offset-end: {}, Header size: {}, Header "
            "(hex): {}",
            entryDesc.keySize, addrEnd.rid().index(), addrEnd.offset(),
            regionManager_.physicalOffset(addrEnd), sizeof(EntryDesc),
            folly::hexlify(
                folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
        return std::nullopt;
      }
      buffer = regionManager_.read(desc, addrEnd.sub(readSize), readSize);
      if (buffer.isNull()) {
        return std::nullopt;
      }

      // modify the address for these variables accordingly
      entryEnd = buffer.data() + buffer.size();
      entryDesc = *reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
    }
  }

  if (entryDesc.csSelf != entryDesc.computeChecksum()) {
    XLOGF(ERR,
          "Item header checksum mismatch in onKeyHashRetrievalFromLocation(). "
          "Region {} is "
          "likely corrupted. Expected: {}, Actual: {}, Offset-end: {}, "
          "Physical-offset-end: {}, Header size: {}, Header (hex): {}",
          addrEnd.rid().index(), entryDesc.csSelf, entryDesc.computeChecksum(),
          addrEnd.offset(), regionManager_.physicalOffset(addrEnd),
          sizeof(EntryDesc),
          folly::hexlify(
              folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
    return std::nullopt;
  }

  folly::StringPiece key{reinterpret_cast<const char*>(
                             entryEnd - sizeof(EntryDesc) - entryDesc.keySize),
                         entryDesc.keySize};
  if (HashedKey(key).keyHash() != entryDesc.keyHash) {
    XLOGF(ERR,
          "The key in Item header doesn't match with the key hash in "
          "onKeyHashRetrievalFromLocation(). "
          "Region {} is "
          "likely corrupted. Expected: {}, Actual: {}, Offset-end: {}, "
          "Physical-offset-end: {}, Header size: {}, Header (hex): {}",
          addrEnd.rid().index(), entryDesc.keyHash, HashedKey(key).keyHash(),
          addrEnd.offset(), regionManager_.physicalOffset(addrEnd),
          sizeof(EntryDesc),
          folly::hexlify(
              folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
    return std::nullopt;
  }

  // Looks like item header is valid. Return the keyHash from the header.
  return entryDesc.keyHash;
}

bool BlockCache::removeItem(HashedKey hk, RelAddress currAddr) {
  if (index_->removeIfMatch(hk.keyHash(), encodeRelAddress(currAddr))) {
    return true;
  }
  evictionLookupMissCounter_.inc();
  return false;
}

/**
 * Record event, key, result, size and info from nvmItem.
 *
 * @param key              The key associated with the event.
 * @param event            The event of type AllocatorApiEvent.
 * @param result           The result of type AllocatorApiResult.
 * @param size             The size of the item in NVM.
 * @param params           Optional struct of type EventRecordParams.
 *
 * @return                 void
 */
void BlockCache::recordEvent(folly::StringPiece key,
                             AllocatorApiEvent event,
                             AllocatorApiResult result,
                             uint32_t size,
                             const NvmItem* nvmItem) {
  auto eventTracker = getEventTracker();
  if (eventTracker) {
    if (!eventTracker->sampleKey(key)) {
      return;
    }
    EventInfo eventInfo;
    eventInfo.event = event;
    eventInfo.result = result;
    eventInfo.size = size;
    eventInfo.key = key;

    // Extract additional information from NvmItem if available
    if (nvmItem) {
      eventInfo.poolId = nvmItem->poolId();
      const auto expiryTime = nvmItem->getExpiryTime();
      const auto creationTime = nvmItem->getCreationTime();

      // expiryTime == 0 means no TTL was set for this item
      if (expiryTime != 0) {
        eventInfo.expiryTime = expiryTime;

        // Calculate configured TTL from expiryTime and creationTime
        // TTL = expiryTime - creationTime
        if (expiryTime > creationTime) {
          eventInfo.ttlSecs = expiryTime - creationTime;
        }

        // Calculate time to expire from current time
        const auto currentTime = util::getCurrentTimeSec();
        if (expiryTime > currentTime) {
          eventInfo.timeToExpire =
              static_cast<uint32_t>(expiryTime - currentTime);
        }
      }

      // Access the allocation size of the item being stored
      if (nvmItem->getNumBlobs() > 0) {
        const auto blob = nvmItem->getBlob(0);
        eventInfo.allocSize = blob.origAllocSize;
      }
    }

    eventTracker->record(eventInfo);
  } else if (legacyEventTracker_.has_value()) {
    legacyEventTracker_->get().record(event, key, result, size);
  }
}

/**
 * Reinsert or remove an item during region reclaim.
 *
 * This function evaluates an item found in a reclaiming region and decides
 * whether to:
 *  1. do nothing if item has been overwritten/replaced, or key not in index.
 *  2. remove the item from index on reinsertion error, expiry or cheksum error.
 *  3. reinsert the item if the reinsertion policy decides to.
 *  4. evict the item.
 *
 * @param hk         HashedKey for the item.
 * @param value      The item's value buffer view within the region.
 * @param entrySize  The serialized size of the item (aligned).
 * @param currAddr   The current relative end address of the item in the region.
 * @param entryDesc  The entry descriptor for the item.
 *
 * @return           AllocatorApiResult indicating the action taken:
 *                   REINSERTED, EVICTED, EXPIRED, REMOVED, FAILED,
 *                   INVALIDATED, NOT_FOUND, or CORRUPTED.
 */
AllocatorApiResult BlockCache::reinsertOrRemoveItem(
    HashedKey hk,
    BufferView value,
    uint32_t entrySize,
    RelAddress currAddr,
    const EntryDesc& entryDesc) {
  // Remove the key from index and return the correct result.
  auto removeItem = [this, hk, currAddr](bool expired) {
    if (index_->removeIfMatch(hk.keyHash(), encodeRelAddress(currAddr))) {
      if (expired) {
        evictionExpiredCount_.inc();
        return AllocatorApiResult::EXPIRED;
      }
      return AllocatorApiResult::EVICTED;
    }
    return AllocatorApiResult::REMOVED;
  };

  const auto lr = index_->peek(hk.keyHash());
  if (!lr.found() || decodeRelAddress(lr.address()) != currAddr) {
    evictionLookupMissCounter_.inc();
    // Either item is not in index (NOT_FOUND) or the time has been
    // replaced by a newer item hence the current item is invalidated
    // (INVALIDATED).
    auto eventRes = !lr.found() ? AllocatorApiResult::NOT_FOUND
                                : AllocatorApiResult::INVALIDATED;
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT, eventRes, entrySize);
    return AllocatorApiResult::REMOVED;
  }

  try {
    if (checkExpired_ && checkExpired_(value)) {
      recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                  AllocatorApiResult::EXPIRED, entrySize);
      return removeItem(true);
    }

    if (!reinsertionPolicy_ ||
        !reinsertionPolicy_->shouldReinsert(hk.key(), toStringPiece(value))) {
      recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                  AllocatorApiResult::REJECTED, entrySize);
      return removeItem(false);
    }
  } catch (const std::exception& e) {
    XLOGF(ERR, "Exception in reinsertOrRemoveItem: {}", e.what());
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                AllocatorApiResult::CORRUPTED, entrySize);
    return AllocatorApiResult::CORRUPTED;
  }

  // Validate checksum as we want to reinsert the item
  if (checksumData_ && entryDesc.cs != checksum(value)) {
    // We do not need to abort here since the EntryDesc checksum was good, so
    // we can safely proceed to read the next entry.
    XLOGF(ERR,
          "Item value checksum mismatch in reinsertOrRemoveItem(). "
          "Item is likely corrupted. Item will not be reinserted. "
          "We will continue evaluating remaining items in the region."
          "Expected: {}, Actual: {}, Value-size: {}, Payload (hex): {}",
          entryDesc.cs,
          checksum(value),
          entryDesc.valueSize,
          folly::hexlify(folly::ByteRange(value.data(), value.dataEnd())));
    reclaimValueChecksumErrorCount_.inc();
    removeItem(false);
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                AllocatorApiResult::CORRUPTED, entrySize);
    return AllocatorApiResult::CORRUPTED;
  }

  // Priority of an re-inserted item is determined by its past accesses
  // since the time it was last (re)inserted.
  // TODO: T95784621 this should be made configurable by having the reinsertion
  //       policy return a priority rank for an item.
  uint16_t priority =
      numPriorities_ == 0
          ? kDefaultItemPriority
          : std::min<uint16_t>(lr.currentHits(), numPriorities_ - 1);

  auto [desc, size, addr] =
      allocateImpl(hk, value.size(), priority, false /* canWait */);

  switch (desc.status()) {
  case OpenStatus::Ready:
    break;
  case OpenStatus::Error:
    allocErrorCount_.inc();
    reinsertionErrorCount_.inc();
    removeItem(false);
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                AllocatorApiResult::FAILED, entrySize);
    return AllocatorApiResult::FAILED;
  case OpenStatus::Retry:
    reinsertionErrorCount_.inc();
    removeItem(false);
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                AllocatorApiResult::FAILED, entrySize);
    return AllocatorApiResult::FAILED;
  }
  auto closeRegionGuard =
      folly::makeGuard([this, desc_2 = std::move(desc)]() mutable {
        allocator_.close(std::move(desc_2));
      });

  // After allocation a region is opened for writing. Until we close it, the
  // region would not be reclaimed and index never gets an invalid entry.
  writeEntry(addr, size, hk, value);
  const auto replaced = index_->replaceIfMatch(hk.keyHash(),
                                               encodeRelAddress(addr.add(size)),
                                               encodeRelAddress(currAddr));
  if (!replaced) {
    recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
                AllocatorApiResult::INVALIDATED, entrySize);
    // happens if you can't find the key in the map
    reinsertionErrorCount_.inc();
    removeItem(false);
    return AllocatorApiResult::FAILED;
  }
  recordEvent(hk.key(), AllocatorApiEvent::NVM_REINSERT,
              AllocatorApiResult::ACCEPTED, entrySize);
  reinsertionCount_.inc();
  reinsertionBytes_.add(entrySize);
  return AllocatorApiResult::REINSERTED;
}

std::tuple<RegionDescriptor, uint32_t, RelAddress> BlockCache::allocateImpl(
    const HashedKey& hk,
    const uint32_t valueSize,
    const uint16_t priority,
    const bool canWait) {
  uint32_t size = serializedSize(hk.key().size(), valueSize);
  auto [desc, addr] =
      allocator_.allocate(size, priority, canWait, hk.keyHash());
  if (desc.isReady()) {
    XDCHECK_LE(addr.offset() + size, regionManager_.regionSize());
    XDCHECK_EQ(size % allocAlignSize_, 0ULL)
        << folly::sformat(" alignSize={}, size={}", allocAlignSize_, size);
  }
  return std::make_tuple(std::move(desc), size, std::move(addr));
}

folly::Expected<std::tuple<RegionDescriptor, uint32_t, RelAddress>, Status>
BlockCache::allocateForInsert(const HashedKey& hk, const uint32_t valueSize) {
  if (serializedSize(hk.key().size(), valueSize) > kMaxItemSize) {
    allocErrorCount_.inc();
    insertCount_.inc();
    return folly::makeUnexpected(Status::Rejected);
  }

  // All newly inserted items are assigned with the lowest priority
  auto retVal =
      allocateImpl(hk, valueSize, kDefaultItemPriority, /* canWait */ true);
  switch (std::get<0>(retVal).status()) {
  case OpenStatus::Ready:
    insertCount_.inc();
    return std::move(retVal);
  case OpenStatus::Error:
    allocErrorCount_.inc();
    insertCount_.inc();
    return folly::makeUnexpected(Status::Rejected);
  case OpenStatus::Retry:
    allocRetryCount_.inc();
    return folly::makeUnexpected(Status::Retry);
  default:
    XLOG(DFATAL) << "Unexpected OpenStatus";
    return folly::makeUnexpected(Status::BadState);
  }
}

/* static */ BlockCache::EntryDesc* BlockCache::writeEntryDescAndKey(
    Buffer& buffer, const HashedKey& hk, uint32_t valueSize) {
  // Copy descriptor and the key to the end
  size_t descOffset = buffer.size() - sizeof(EntryDesc);
  auto desc = new (buffer.data() + descOffset)
      EntryDesc(hk.key().size(), valueSize, hk.keyHash());
  buffer.copyFrom(descOffset - hk.key().size(), makeView(hk.key()));
  return desc;
}

void BlockCache::writeEntry(RelAddress addr,
                            uint32_t size,
                            HashedKey hk,
                            BufferView value) {
  auto buffer = Buffer(size);
  auto* desc = writeEntryDescAndKey(buffer, hk, value.size());
  if (checksumData_) {
    desc->cs = checksum(value);
  }
  buffer.copyFrom(0, value);

  regionManager_.write(addr, std::move(buffer));
  logicalWrittenCount_.add(hk.key().size() + value.size());
}

Status BlockCache::readEntry(LookupData& ld,
                             RelAddress addrEnd,
                             uint32_t approxSize,
                             HashedKey expected) {
  // Because region opened for read, nobody will reclaim it or modify. Safe
  // without locks.

  // The item layout is as thus
  // | --- value --- | --- empty --- | --- header --- |
  // The size itself is determined by serializedSize(), so
  // we will try to read exactly that size or just slightly over.

  // Because we either use a predefined read buffer size, or align the size
  // up by kMinAllocAlignSize, our size might be bigger than the actual item
  // size. So we need to ensure we're not reading past the region's beginning.
  approxSize = std::min(approxSize, addrEnd.offset());

  XDCHECK_EQ(approxSize % allocAlignSize_, 0ULL) << folly::sformat(
      " alignSize={}, approxSize={}", allocAlignSize_, approxSize);

  // Because we are going to look for EntryDesc in the buffer read, the buffer
  // must be atleast as big as EntryDesc aligned to next 2 power
  XDCHECK_GE(approxSize, folly::nextPowTwo(sizeof(EntryDesc)));

  auto buffer =
      regionManager_.read(ld.desc_, addrEnd.sub(approxSize), approxSize);
  if (buffer.isNull()) {
    return Status::DeviceError;
  }

  auto entryEnd = buffer.data() + buffer.size();
  auto desc = *reinterpret_cast<EntryDesc*>(entryEnd - sizeof(EntryDesc));
  if (desc.csSelf != desc.computeChecksum()) {
    lookupEntryHeaderChecksumErrorCount_.inc();
    XLOG_N_PER_MS(ERR, 10, 10'000) << folly::sformat(
        "Header checksum mismatch in readEntry() at Region: {}, Expected: {}, "
        "Actual: {}, Offset-end: {}, Physical-offset-end: {}, Header size: {}, "
        "Header (hex): {}",
        addrEnd.rid().index(), desc.csSelf, desc.computeChecksum(),
        addrEnd.offset(), regionManager_.physicalOffset(addrEnd),
        sizeof(EntryDesc),
        folly::hexlify(
            folly::ByteRange(entryEnd - sizeof(EntryDesc), entryEnd)));
    return Status::ChecksumError;
  }

  folly::StringPiece key{reinterpret_cast<const char*>(
                             entryEnd - sizeof(EntryDesc) - desc.keySize),
                         desc.keySize};
  if (HashedKey::precomputed(key, desc.keyHash) != expected) {
    lookupFalsePositiveCount_.inc();
    return Status::NotFound;
  }

  // Update the size to actual, defined by key and value size
  uint32_t size = serializedSize(desc.keySize, desc.valueSize);
  if (buffer.size() > size) {
    // Read more than actual size. Trim the invalid data in the beginning
    excessiveReadBytes_.add(buffer.size() - size);
    buffer.trimStart(buffer.size() - size);
  } else if (buffer.size() < size) {
    // Read less than actual size. Read again with proper buffer.
    buffer = regionManager_.read(ld.desc_, addrEnd.sub(size), size);
    if (buffer.isNull()) {
      return Status::DeviceError;
    }
  }

  ld.buffer_ = std::move(buffer);
  ld.valueSize_ = desc.valueSize;
  auto slice = ld.buffer_.view().slice(0, desc.valueSize);
  if (checksumData_ && desc.cs != checksum(slice)) {
    XLOG_N_PER_MS(ERR, 10, 10'000) << folly::sformat(
        "Item value checksum mismatch in readEntry() looking up key {} in "
        "Region {}. Expected: {}, Actual: {}, Offset: {}, Physical-offset: {}, "
        "Value-size: {} Payload (hex): {}",
        key, addrEnd.rid().index(), desc.cs, checksum(slice),
        addrEnd.offset() - size, regionManager_.physicalOffset(addrEnd) - size,
        slice.size(),
        folly::hexlify(
            folly::ByteRange(slice.data(), slice.data() + slice.size())));
    ld.buffer_.reset();
    lookupValueChecksumErrorCount_.inc();
    return Status::ChecksumError;
  }
  return Status::Ok;
}

void BlockCache::drain() { regionManager_.drain(); }

void BlockCache::flush() {
  XLOG(INFO, "Flush block cache");
  allocator_.flush();
  regionManager_.flush();
}

void BlockCache::reset() {
  XLOG(INFO, "Reset block cache");
  index_->reset();
  initializeBlockCache();
}

void BlockCache::initializeBlockCache() {
  // Allocator resets region manager
  allocator_.reset();

  // Reset counters
  insertCount_.set(0);
  lookupCount_.set(0);
  removeCount_.set(0);
  allocErrorCount_.set(0);
  allocRetryCount_.set(0);
  logicalWrittenCount_.set(0);
  holeCount_.set(0);
  holeSizeTotal_.set(0);
  usedSizeBytes_.set(0);
}

void BlockCache::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_size", getSize());
  visitor("navy_bc_items", index_->computeSize());
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
  visitor("navy_bc_retry_reads", retryReadCount_.get(),
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
  visitor("navy_bc_alloc_retries", allocRetryCount_.get(),
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
  visitor("navy_bc_excessive_read_bytes", excessiveReadBytes_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_lookup_for_item_destructor_errors",
          lookupForItemDestructorErrorCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_bc_remove_attempt_collisions", removeAttemptCollisions_.get(),
          CounterVisitor::CounterType::RATE);
  bcLifetimeSecs_.visitQuantileEstimator(visitor, "navy_bc_item_lifetime_secs");
  insertLatency_.visitQuantileEstimator(visitor, "navy_bc_insert_latency_us");
  lookupLatency_.visitQuantileEstimator(visitor, "navy_bc_lookup_latency_us");
  removeLatency_.visitQuantileEstimator(visitor, "navy_bc_remove_latency_us");
  // Allocator visits region manager
  allocator_.getCounters(visitor);
  index_->getCounters(visitor);

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
  index_->persist(rw);

  XLOG(INFO, "Finished block cache persist");
}

bool BlockCache::recover(RecordReader& rr) {
  XLOG(INFO, "Starting block cache recovery");
  initializeBlockCache();
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
  index_->recover(rr);
}

bool BlockCache::isValidRecoveryData(
    const serialization::BlockCacheConfig& recoveredConfig) const {
  return *config_.cacheBaseOffset() == *recoveredConfig.cacheBaseOffset() &&
         *config_.cacheSize() == *recoveredConfig.cacheSize() &&
         static_cast<int32_t>(allocAlignSize_) ==
             *recoveredConfig.allocAlignSize() &&
         *config_.checksum() == *recoveredConfig.checksum() &&
         *config_.version() == *recoveredConfig.version();
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

void BlockCache::addHole(uint32_t size) const {
  holeCount_.inc();
  holeSizeTotal_.add(size);
}
void BlockCache::removeHole(uint32_t size) const {
  holeCount_.sub(1);
  holeSizeTotal_.sub(size);
}

} // namespace facebook::cachelib::navy

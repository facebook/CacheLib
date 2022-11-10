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

#include "cachelib/common/Hash.h"
namespace facebook {
namespace cachelib {

template <typename C>
std::map<std::string, std::string> NvmCache<C>::Config::serialize() const {
  std::map<std::string, std::string> configMap;
  configMap = navyConfig.serialize();
  configMap["encodeCB"] = encodeCb ? "set" : "empty";
  configMap["decodeCb"] = decodeCb ? "set" : "empty";
  configMap["encryption"] = deviceEncryptor ? "set" : "empty";
  configMap["truncateItemToOriginalAllocSizeInNvm"] =
      truncateItemToOriginalAllocSizeInNvm ? "true" : "false";
  return configMap;
}

template <typename C>
typename NvmCache<C>::Config NvmCache<C>::Config::validateAndSetDefaults() {
  const bool hasEncodeCb = !!encodeCb;
  const bool hasDecodeCb = !!decodeCb;
  if (hasEncodeCb != hasDecodeCb) {
    throw std::invalid_argument(
        "Encode and Decode CBs must be both specified or both empty.");
  }

  if (deviceEncryptor) {
    auto encryptionBlockSize = deviceEncryptor->encryptionBlockSize();
    auto blockSize = navyConfig.getBlockSize();
    if (blockSize % encryptionBlockSize != 0) {
      throw std::invalid_argument(folly::sformat(
          "Encryption enabled but the encryption block granularity is not "
          "aligned to the navy block size. ecryption block size: {}, "
          "block size: {}",
          encryptionBlockSize,
          blockSize));
    }

    if (navyConfig.isBigHashEnabled()) {
      auto bucketSize = navyConfig.bigHash().getBucketSize();
      if (bucketSize % encryptionBlockSize != 0) {
        throw std::invalid_argument(
            folly::sformat("Encryption enabled but the encryption block "
                           "granularity is not aligned to the navy "
                           "big hash bucket size. ecryption block "
                           "size: {}, bucket size: {}",
                           encryptionBlockSize,
                           bucketSize));
      }
    }
  }

  return *this;
}

template <typename C>
typename NvmCache<C>::DeleteTombStoneGuard NvmCache<C>::createDeleteTombStone(
    HashedKey hk) {
  // lower bits for shard and higher bits for key.
  const auto shard = hk.keyHash() % kShards;
  auto guard = tombstones_[shard].add(hk.key());

  // need to synchronize tombstone creations with fill lock to serialize
  // async fills with deletes
  // o/w concurrent onGetComplete might re-insert item to RAM after tombstone
  // is created.
  // The lock here is to that  adding the tombstone and checking the dram cache
  // in remove path happens entirely before or  happens entirely after checking
  // for tombstone and inserting into dram from the get path.
  // If onGetComplete is holding the FillLock, wait the insertion to be done.
  // If onGetComplete has not yet acquired FillLock, we are fine to exit since
  // hasTombStone in onGetComplete is checked after acquiring FillLock.
  auto lock = getFillLockForShard(shard);

  return guard;
}

template <typename C>
bool NvmCache<C>::hasTombStone(HashedKey hk) {
  // lower bits for shard and higher bits for key.
  const auto shard = hk.keyHash() % kShards;
  return tombstones_[shard].isPresent(hk.key());
}

template <typename C>
typename NvmCache<C>::WriteHandle NvmCache<C>::find(HashedKey hk) {
  if (!isEnabled()) {
    return WriteHandle{};
  }

  util::LatencyTracker tracker(stats().nvmLookupLatency_);

  auto shard = getShardForKey(hk);
  // invalidateToken any inflight puts for the same key since we are filling
  // from nvmcache.
  inflightPuts_[shard].invalidateToken(hk.key());

  stats().numNvmGets.inc();

  GetCtx* ctx{nullptr};
  WriteHandle hdl{nullptr};
  {
    auto lock = getFillLockForShard(shard);
    // do not use the Cache::find() since that will call back into us.
    hdl = CacheAPIWrapperForNvm<C>::findInternal(cache_, hk.key());
    if (UNLIKELY(hdl != nullptr)) {
      if (hdl->isExpired()) {
        hdl.reset();
        hdl.markExpired();
        stats().numNvmGetMissExpired.inc();
        stats().numNvmGetMissFast.inc();
      }
      return hdl;
    }

    auto& fillMap = getFillMapForShard(shard);
    auto it = fillMap.find(hk.key());
    // we use async apis for nvmcache operations into navy. async apis for
    // lookups incur additional overheads and thread hops. However, navy can
    // quickly answer negative lookups through a synchronous api. So we try to
    // quickly validate this, if possible, before doing the heavier async
    // lookup.
    //
    // since this is a synchronous api, navy would not guarantee any
    // particular ordering semantic with other concurrent requests to same
    // key. we need to ensure there are no asynchronous api requests for the
    // same key. First, if there are already concurrent get requests, we
    // simply add ourselves to the list of waiters for that get. If there are
    // concurrent put requests already enqueued, executing this synchronous
    // api can read partial state. Hence the result can not be trusted. If
    // there are concurrent delete requests enqueued, we might get false
    // positives that key is present. That is okay since it is a loss of
    // performance and not correctness.
    //
    // For concurrent put, if it is already enqueued, its put context already
    // exists. If it is not enqueued yet (in-flight) the above invalidateToken
    // will prevent the put from being enqueued.
    if (config_.enableFastNegativeLookups && it == fillMap.end() &&
        !putContexts_[shard].hasContexts() && !navyCache_->couldExist(hk)) {
      stats().numNvmGetMiss.inc();
      stats().numNvmGetMissFast.inc();
      return WriteHandle{};
    }

    hdl = CacheAPIWrapperForNvm<C>::createNvmCacheFillHandle(cache_);
    hdl.markWentToNvm();

    auto waitContext = CacheAPIWrapperForNvm<C>::getWaitContext(cache_, hdl);
    XDCHECK(waitContext);

    if (it != fillMap.end()) {
      ctx = it->second.get();
      ctx->addWaiter(std::move(waitContext));
      stats().numNvmGetCoalesced.inc();
      return hdl;
    }

    // create a context
    auto newCtx = std::make_unique<GetCtx>(
        *this, hk.key(), std::move(waitContext), std::move(tracker));
    auto res =
        fillMap.emplace(std::make_pair(newCtx->getKey(), std::move(newCtx)));
    XDCHECK(res.second);
    ctx = res.first->second.get();
  } // scope for fill lock

  XDCHECK(ctx);
  auto guard = folly::makeGuard([hk, this]() { removeFromFillMap(hk); });

  navyCache_->lookupAsync(
      HashedKey::precomputed(ctx->getKey(), hk.keyHash()),
      [this, ctx](navy::Status s, HashedKey k, navy::Buffer v) {
        this->onGetComplete(*ctx, s, k, v.view());
      });
  guard.dismiss();
  return hdl;
}

template <typename C>
bool NvmCache<C>::couldExistFast(HashedKey hk) {
  if (!isEnabled()) {
    return false;
  }

  auto shard = getShardForKey(hk);
  // invalidateToken any inflight puts for the same key since we are filling
  // from nvmcache.
  inflightPuts_[shard].invalidateToken(hk.key());

  auto lock = getFillLockForShard(shard);
  // do not use the Cache::find() since that will call back into us.
  auto hdl = CacheAPIWrapperForNvm<C>::findInternal(cache_, hk.key());
  if (hdl != nullptr) {
    if (hdl->isExpired()) {
      return false;
    }
    return true;
  }

  auto& fillMap = getFillMapForShard(shard);
  auto it = fillMap.find(hk.key());
  // we use async apis for nvmcache operations into navy. async apis for
  // lookups incur additional overheads and thread hops. However, navy can
  // quickly answer negative lookups through a synchronous api. So we try to
  // quickly validate this, if possible, before doing the heavier async
  // lookup.
  //
  // since this is a synchronous api, navy would not guarantee any
  // particular ordering semantic with other concurrent requests to same
  // key. we need to ensure there are no asynchronous api requests for the
  // same key. First, if there are already concurrent get requests, we
  // simply add ourselves to the list of waiters for that get. If there are
  // concurrent put requests already enqueued, executing this synchronous
  // api can read partial state. Hence the result can not be trusted. If
  // there are concurrent delete requests enqueued, we might get false
  // positives that key is present. That is okay since it is a loss of
  // performance and not correctness.
  //
  // For concurrent put, if it is already enqueued, its put context already
  // exists. If it is not enqueued yet (in-flight) the above invalidateToken
  // will prevent the put from being enqueued.
  if (config_.enableFastNegativeLookups && it == fillMap.end() &&
      !putContexts_[shard].hasContexts() && !navyCache_->couldExist(hk)) {
    return false;
  }

  return true;
}

template <typename C>
typename NvmCache<C>::WriteHandle NvmCache<C>::peek(folly::StringPiece key) {
  if (!isEnabled()) {
    return nullptr;
  }

  folly::Baton b;
  WriteHandle hdl{};
  hdl.markWentToNvm();

  // no need for fill lock or inspecting the state of other concurrent
  // operations since we only want to check the state for debugging purposes.
  navyCache_->lookupAsync(
      HashedKey{key}, [&, this](navy::Status st, HashedKey, navy::Buffer v) {
        if (st != navy::Status::NotFound) {
          auto nvmItem = reinterpret_cast<const NvmItem*>(v.data());
          hdl = createItem(key, *nvmItem);
        }
        b.post();
      });
  b.wait();
  return hdl;
}

// invalidate any inflight lookup that is on flight since we are evicting it.
template <typename C>
void NvmCache<C>::evictCB(HashedKey hk,
                          navy::BufferView value,
                          navy::DestructorEvent event) {
  // invalidate any inflight lookup that is on flight since we are evicting it.
  invalidateFill(hk);

  const auto& nvmItem = *reinterpret_cast<const NvmItem*>(value.data());

  if (event == cachelib::navy::DestructorEvent::Recycled) {
    // Recycled means item is evicted
    // update stats for eviction
    stats().numNvmEvictions.inc();

    const auto timeNow = util::getCurrentTimeSec();
    const auto lifetime = timeNow - nvmItem.getCreationTime();
    const auto expiryTime = nvmItem.getExpiryTime();
    if (expiryTime != 0) {
      if (expiryTime < timeNow) {
        stats().numNvmExpiredEvict.inc();
        stats().nvmEvictionSecondsPastExpiry_.trackValue(timeNow - expiryTime);
      } else {
        stats().nvmEvictionSecondsToExpiry_.trackValue(expiryTime - timeNow);
      }
    }
    navyCache_->isItemLarge(hk, value)
        ? stats().nvmLargeLifetimeSecs_.trackValue(lifetime)
        : stats().nvmSmallLifetimeSecs_.trackValue(lifetime);
    if (auto eventTracker = CacheAPIWrapperForNvm<C>::getEventTracker(cache_)) {
      eventTracker->record(AllocatorApiEvent::NVM_EVICT, hk.key(),
                           AllocatorApiResult::EVICTED);
    }
  }

  bool needDestructor = true;
  {
    // The ItemDestructorLock is to protect:
    // 1. peek item in DRAM cache,
    // 2. check it's NvmClean flag
    // 3. mark NvmEvicted flag
    // 4. lookup itemRemoved_ set.
    // Concurrent DRAM cache remove/replace/update for same item could
    // modify DRAM index, check NvmClean/NvmEvicted flag, update itemRemoved_
    // set, and unmark NvmClean flag.
    auto lock = getItemDestructorLock(hk);
    WriteHandle hdl;
    try {
      hdl = WriteHandle{cache_.peek(hk.key())};
    } catch (const exception::RefcountOverflow& ex) {
      // TODO(zixuan) item exists in DRAM, but we can't obtain the handle
      // and mark it as NvmEvicted. In this scenario, there are two
      // possibilities when the item is removed from nvm.
      // 1. destructor is not executed: The item in DRAM is still marked
      // NvmClean, so when it is evicted from DRAM, destructor is also skipped
      // since we infer nvm copy exists  (NvmClean && !NvmEvicted). In this
      // case, we incorrectly skip executing an item destructor and it is also
      // possible to leak the itemRemoved_ state if this item is
      // removed/replaced from DRAM before this happens.
      // 2. destructor is executed here: In addition to destructor being
      // executed here, it could also be executed if the item was removed from
      // DRAM and the handle goes out of scope. Among the two, (1) is preferred,
      // until we can solve this, since executing destructor here while item
      // handle being outstanding and being possibly used is dangerous.
      XLOGF(ERR,
            "Refcount overflowed when trying peek at an item in "
            "NvmCache::evictCB. key: {}, ex: {}",
            hk.key(),
            ex.what());
      stats().numNvmDestructorRefcountOverflow.inc();
      return;
    }

    if (hdl && hdl->isNvmClean()) {
      // item found in RAM and it is NvmClean
      // this means it is the same copy as what we are evicting/removing
      needDestructor = false;
      if (hdl->isNvmEvicted()) {
        // this means we evicted something twice. This should not happen even we
        // could have two copies in the nvm cache, since we only have one copy
        // in index, the one not in index should not reach here.
        stats().numNvmCleanDoubleEvict.inc();
      } else {
        hdl->markNvmEvicted();
        stats().numNvmCleanEvict.inc();
      }
    } else {
      if (hdl) {
        // item found in RAM but is NOT NvmClean
        // this happens when RAM copy is in-place updated, or replaced with a
        // new item.
        stats().numNvmUncleanEvict.inc();
      }

      // If we can't find item from DRAM or isNvmClean flag not set, it might be
      // removed/replaced. Check if it is in itemRemoved_, item existing in
      // itemRemoved_ means it was in DRAM, was removed/replaced and
      // destructor should have been executed by the DRAM copy.
      //
      // PutFailed event can skip the check because when item was in flight put
      // and failed it was the latest copy and item was not removed/replaced
      // but it could exist in itemRemoved_ due to in-place mutation and the
      // legacy copy in NVM is still pending to be removed.
      if (event != cachelib::navy::DestructorEvent::PutFailed &&
          checkAndUnmarkItemRemovedLocked(hk)) {
        needDestructor = false;
      }
    }
  }

  if (!needDestructor) {
    return;
  }

  if (event != cachelib::navy::DestructorEvent::Removed) {
    stats().numCacheEvictions.inc();
  }
  // ItemDestructor
  if (itemDestructor_) {
    // create the item on heap instead of memory pool to avoid allocation
    // failure and evictions from cache for a temporary item.
    auto iobuf = createItemAsIOBuf(hk.key(), nvmItem);
    if (iobuf) {
      auto& item = *reinterpret_cast<Item*>(iobuf->writableData());
      // make chained items
      auto chained = viewAsChainedAllocsRange(iobuf.get());
      auto context = event == cachelib::navy::DestructorEvent::Removed
                         ? DestructorContext::kRemovedFromNVM
                         : DestructorContext::kEvictedFromNVM;

      try {
        itemDestructor_(DestructorData{context, item, std::move(chained),
                                       nvmItem.poolId()});
        stats().numNvmDestructorCalls.inc();
      } catch (const std::exception& e) {
        stats().numDestructorExceptions.inc();
        XLOG_EVERY_N(INFO, 100)
            << "Catch exception from user's item destructor: " << e.what();
      }
    }
  }
}

template <typename C>
folly::Range<typename C::ChainedItemIter> NvmCache<C>::viewAsChainedAllocsRange(
    folly::IOBuf* parent) const {
  XDCHECK(parent);
  auto& item = *reinterpret_cast<Item*>(parent->writableData());
  return item.hasChainedItem()
             ? folly::Range<ChainedItemIter>{ChainedItemIter{parent->next()},
                                             ChainedItemIter{parent}}
             : folly::Range<ChainedItemIter>{};
}

template <typename C>
NvmCache<C>::NvmCache(C& c,
                      Config config,
                      bool truncate,
                      const ItemDestructor& itemDestructor)
    : config_(config.validateAndSetDefaults()),
      cache_(c),
      itemDestructor_(itemDestructor) {
  navyCache_ = createNavyCache(
      config_.navyConfig,
      [](navy::BufferView v) -> bool {
        const auto& nvmItem = *reinterpret_cast<const NvmItem*>(v.data());
        return nvmItem.isExpired();
      },
      [this](HashedKey hk, navy::BufferView v, navy::DestructorEvent e) {
        this->evictCB(hk, v, e);
      },
      truncate,
      std::move(config.deviceEncryptor),
      itemDestructor_ ? true : false);
}

template <typename C>
Blob NvmCache<C>::makeBlob(const Item& it) {
  return Blob{
      // User requested size
      it.getSize(),
      // Storage size in NvmCache may be greater than user-requested-size
      // if nvmcache is configured with useTruncatedAllocSize == false
      {reinterpret_cast<const char*>(it.getMemory()), getStorageSizeInNvm(it)}};
}

template <typename C>
uint32_t NvmCache<C>::getStorageSizeInNvm(const Item& it) {
  return config_.truncateItemToOriginalAllocSizeInNvm
             ? it.getSize()
             : cache_.getUsableSize(it);
}

template <typename C>
std::unique_ptr<NvmItem> NvmCache<C>::makeNvmItem(const WriteHandle& hdl) {
  const auto& item = *hdl;
  auto poolId = cache_.getAllocInfo((void*)(&item)).poolId;

  if (item.isChainedItem()) {
    throw std::invalid_argument(folly::sformat(
        "Chained item can not be flushed separately {}", hdl->toString()));
  }

  auto chainedItemRange =
      CacheAPIWrapperForNvm<C>::viewAsChainedAllocsRange(cache_, *hdl);
  if (config_.encodeCb && !config_.encodeCb(EncodeDecodeContext{
                              *(hdl.getInternal()), chainedItemRange})) {
    return nullptr;
  }

  if (item.hasChainedItem()) {
    std::vector<Blob> blobs;
    blobs.push_back(makeBlob(item));

    for (auto& chainedItem : chainedItemRange) {
      blobs.push_back(makeBlob(chainedItem));
    }

    const size_t bufSize = NvmItem::estimateVariableSize(blobs);
    return std::unique_ptr<NvmItem>(new (bufSize) NvmItem(
        poolId, item.getCreationTime(), item.getExpiryTime(), blobs));
  } else {
    Blob blob = makeBlob(item);
    const size_t bufSize = NvmItem::estimateVariableSize(blob);
    return std::unique_ptr<NvmItem>(new (bufSize) NvmItem(
        poolId, item.getCreationTime(), item.getExpiryTime(), blob));
  }
}

template <typename C>
void NvmCache<C>::put(WriteHandle& hdl, PutToken token) {
  util::LatencyTracker tracker(stats().nvmInsertLatency_);
  HashedKey hk{hdl->getKey()};

  XDCHECK(hdl);
  auto& item = *hdl;
  // for regular items that can only write to nvmcache upon eviction, we
  // should not be recording a write for an nvmclean item unless it is marked
  // as evicted from nvmcache.
  if (item.isNvmClean() && !item.isNvmEvicted()) {
    throw std::runtime_error(folly::sformat(
        "Item is not nvm evicted and nvm clean {}", item.toString()));
  }

  if (item.isChainedItem()) {
    throw std::invalid_argument(
        folly::sformat("Invalid item {}", item.toString()));
  }

  // we skip writing if we know that the item is expired or has chained items
  if (!isEnabled() || item.isExpired()) {
    return;
  }

  stats().numNvmPuts.inc();
  if (hasTombStone(hk)) {
    stats().numNvmAbortedPutOnTombstone.inc();
    return;
  }

  auto nvmItem = makeNvmItem(hdl);
  if (!nvmItem) {
    stats().numNvmPutEncodeFailure.inc();
    return;
  }

  if (item.isNvmClean() && item.isNvmEvicted()) {
    stats().numNvmPutFromClean.inc();
  }

  auto iobuf = toIOBuf(std::move(nvmItem));
  const auto valSize = iobuf.length();
  auto val = folly::ByteRange{iobuf.data(), iobuf.length()};

  auto shard = getShardForKey(hk);
  auto& putContexts = putContexts_[shard];
  auto& ctx = putContexts.createContext(item.getKey(), std::move(iobuf),
                                        std::move(tracker));
  // capture array reference for putContext. it is stable
  auto putCleanup = [&putContexts, &ctx]() { putContexts.destroyContext(ctx); };
  auto guard = folly::makeGuard([putCleanup]() { putCleanup(); });

  // On a concurrent get, we remove the key from inflight evictions and hence
  // key not being present means a concurrent get happened with an inflight
  // eviction, and we should abandon this write to navy since we already
  // reported the key doesn't exist in the cache.
  const bool executed = token.executeIfValid([&]() {
    auto status = navyCache_->insertAsync(
        HashedKey::precomputed(ctx.key(), hk.keyHash()), makeBufferView(val),
        [this, putCleanup, valSize, val](navy::Status st, HashedKey key) {
          if (st == navy::Status::Ok) {
            stats().nvmPutSize_.trackValue(valSize);
          } else if (st == navy::Status::BadState) {
            // we set disable navy since we got a BadState from navy
            disableNavy("Delete Failure. BadState");
          } else {
            // put failed, DRAM eviction happened and destructor was not
            // executed. we unconditionally trigger destructor here for cleanup.
            evictCB(key, makeBufferView(val), navy::DestructorEvent::PutFailed);
          }
          putCleanup();
        });

    if (status == navy::Status::Ok) {
      guard.dismiss();
      // mark it as NvmClean and unNvmEvicted if we put it into the queue
      // so handle destruction awares that there's a NVM copy (at least in the
      // queue)
      item.markNvmClean();
      item.unmarkNvmEvicted();
    } else {
      stats().numNvmPutErrs.inc();
    }
  });

  // if insertAsync is not executed or put into scheduler queue successfully,
  // NvmClean is not marked for the item, destructor of the item will be invoked
  // upon handle release.
  if (!executed) {
    stats().numNvmAbortedPutOnInflightGet.inc();
  }
}

template <typename C>
typename NvmCache<C>::PutToken NvmCache<C>::createPutToken(
    folly::StringPiece key) {
  return inflightPuts_[getShardForKey(key)].tryAcquireToken(key);
}

template <typename C>
void NvmCache<C>::onGetComplete(GetCtx& ctx,
                                navy::Status status,
                                HashedKey hk,
                                navy::BufferView val) {
  auto guard =
      folly::makeGuard([&ctx, hk]() { ctx.cache.removeFromFillMap(hk); });
  // navy got disabled while we were fetching. If so, safely return a miss.
  // If navy gets disabled beyond this point, it is okay since we fetched it
  // before we got disabled.
  if (!isEnabled()) {
    return;
  }

  if (status != navy::Status::Ok) {
    // instead of disabling navy, we enqueue a delete and return a miss.
    if (status != navy::Status::NotFound) {
      remove(hk, createDeleteTombStone(hk));
    }
    stats().numNvmGetMiss.inc();
    return;
  }

  const NvmItem* nvmItem = reinterpret_cast<const NvmItem*>(val.data());

  // this item expired. return a miss.
  if (nvmItem->isExpired()) {
    stats().numNvmGetMiss.inc();
    stats().numNvmGetMissExpired.inc();
    WriteHandle hdl{};
    hdl.markExpired();
    hdl.markWentToNvm();
    ctx.setWriteHandle(std::move(hdl));
    return;
  }

  auto it = createItem(hk.key(), *nvmItem);
  if (!it) {
    stats().numNvmGetMiss.inc();
    stats().numNvmGetMissErrs.inc();
    // we failed to fill due to an internal failure. Return a miss and
    // invalidate what we have in nvmcache
    remove(hk, createDeleteTombStone(hk));
    return;
  }

  XDCHECK(it->isNvmClean());

  auto lock = getFillLock(hk);
  if (hasTombStone(hk) || !ctx.isValid()) {
    // a racing remove or evict while we were filling
    stats().numNvmGetMiss.inc();
    stats().numNvmGetMissDueToInflightRemove.inc();
    return;
  }

  // by the time we filled from navy, another thread inserted in RAM. We
  // disregard.
  if (CacheAPIWrapperForNvm<C>::insertFromNvm(cache_, it)) {
    it.markWentToNvm();
    ctx.setWriteHandle(std::move(it));
  }
} // namespace cachelib

template <typename C>
typename NvmCache<C>::WriteHandle NvmCache<C>::createItem(
    folly::StringPiece key, const NvmItem& nvmItem) {
  const size_t numBufs = nvmItem.getNumBlobs();
  // parent item
  XDCHECK_GE(numBufs, 1u);
  const auto pBlob = nvmItem.getBlob(0);

  stats().numNvmAllocAttempts.inc();
  // use the original alloc size to allocate, but make sure that the usable
  // size matches the pBlob's size
  auto it = CacheAPIWrapperForNvm<C>::allocateInternal(
      cache_, nvmItem.poolId(), key, pBlob.origAllocSize,
      nvmItem.getCreationTime(), nvmItem.getExpiryTime());
  if (!it) {
    return nullptr;
  }

  XDCHECK_LE(pBlob.data.size(), getStorageSizeInNvm(*it));
  XDCHECK_LE(pBlob.origAllocSize, pBlob.data.size());
  ::memcpy(it->getMemory(), pBlob.data.data(), pBlob.data.size());
  it->markNvmClean();

  // if we have more, then we need to allocate them as chained items and add
  // them in the same order. To do that, we need to add them from the inverse
  // order
  if (numBufs > 1) {
    // chained items need to be added in reverse order to maintain the same
    // order as what we serialized.
    for (int i = numBufs - 1; i >= 1; i--) {
      auto cBlob = nvmItem.getBlob(i);
      XDCHECK_GT(cBlob.origAllocSize, 0u);
      XDCHECK_GT(cBlob.data.size(), 0u);
      stats().numNvmAllocAttempts.inc();
      auto chainedIt = cache_.allocateChainedItem(it, cBlob.origAllocSize);
      if (!chainedIt) {
        return nullptr;
      }
      XDCHECK(chainedIt->isChainedItem());
      XDCHECK_LE(cBlob.data.size(), getStorageSizeInNvm(*chainedIt));
      ::memcpy(chainedIt->getMemory(), cBlob.data.data(), cBlob.data.size());
      cache_.addChainedItem(it, std::move(chainedIt));
      XDCHECK(it->hasChainedItem());
    }
  }

  // issue the call back to decode and fix up the item if needed.
  if (config_.decodeCb) {
    config_.decodeCb(EncodeDecodeContext{
        *it, CacheAPIWrapperForNvm<C>::viewAsChainedAllocsRange(cache_, *it)});
  }
  return it;
}

template <typename C>
std::unique_ptr<folly::IOBuf> NvmCache<C>::createItemAsIOBuf(
    folly::StringPiece key, const NvmItem& nvmItem, bool parentOnly) {
  const size_t numBufs = parentOnly ? 1 : nvmItem.getNumBlobs();
  // parent item
  XDCHECK_GE(numBufs, 1u);
  const auto pBlob = nvmItem.getBlob(0);

  stats().numNvmAllocForItemDestructor.inc();
  std::unique_ptr<folly::IOBuf> head;

  try {
    // Use the pBlob's actual size instead of origAllocSize
    // because the slack space might be used if nvmcache is configured
    // with useTruncatedAllocSize == false
    XDCHECK_LE(pBlob.origAllocSize, pBlob.data.size());
    auto size = Item::getRequiredSize(key, pBlob.data.size());

    head = folly::IOBuf::create(size);
    head->append(size);
  } catch (const std::bad_alloc&) {
    stats().numNvmItemDestructorAllocErrors.inc();
    return nullptr;
  }
  auto item = new (head->writableData())
      Item(key, pBlob.origAllocSize, nvmItem.getCreationTime(),
           nvmItem.getExpiryTime());

  XDCHECK_LE(pBlob.origAllocSize, item->getSize());
  XDCHECK_LE(pBlob.origAllocSize, pBlob.data.size());
  ::memcpy(item->getMemory(), pBlob.data.data(), pBlob.data.size());
  item->markNvmClean();
  item->markNvmEvicted();

  // if we have more, then we need to allocate them as chained items and add
  // them in the same order. To do that, we need to add them from the inverse
  // order
  if (numBufs > 1) {
    // chained items need to be added in reverse order to maintain the same
    // order as what we serialized.
    for (int i = numBufs - 1; i >= 1; i--) {
      auto cBlob = nvmItem.getBlob(i);
      XDCHECK_GT(cBlob.origAllocSize, 0u);
      XDCHECK_GT(cBlob.data.size(), 0u);
      stats().numNvmAllocForItemDestructor.inc();
      std::unique_ptr<folly::IOBuf> chained;
      try {
        auto size = ChainedItem::getRequiredSize(cBlob.origAllocSize);
        chained = folly::IOBuf::create(size);
        chained->append(size);
      } catch (const std::bad_alloc&) {
        stats().numNvmItemDestructorAllocErrors.inc();
        return nullptr;
      }
      auto chainedItem = new (chained->writableData()) ChainedItem(
          CompressedPtr(), cBlob.origAllocSize, util::getCurrentTimeSec());
      XDCHECK(chainedItem->isChainedItem());
      ::memcpy(chainedItem->getMemory(), cBlob.data.data(),
               cBlob.origAllocSize);
      head->appendChain(std::move(chained));
      item->markHasChainedItem();
      XDCHECK(item->hasChainedItem());
    }
  }
  return head;
}

template <typename C>
void NvmCache<C>::disableNavy(const std::string& msg) {
  if (isEnabled()) {
    navyEnabled_ = false;
    XLOGF(CRITICAL, "Disabling navy. {}", msg);
  }
}

template <typename C>
void NvmCache<C>::remove(HashedKey hk, DeleteTombStoneGuard tombstone) {
  if (!isEnabled()) {
    return;
  }
  XDCHECK(tombstone);

  stats().numNvmDeletes.inc();

  util::LatencyTracker tracker(stats().nvmRemoveLatency_);
  const auto shard = getShardForKey(hk);
  //
  // invalidate any inflight put that is on flight since we are queueing up a
  // deletion.
  inflightPuts_[shard].invalidateToken(hk.key());

  // Skip scheduling async job to remove the key if the key couldn't exist,
  // if there are no put requests for the key shard.
  //
  // The existence check for skipping a remove to be enqueued is not going to
  // be changed by a get. It can be changed only by a concurrent put. And to
  // co-ordinate with that, we need to ensure that there are no put contexts
  // (in-flight puts) before we check for couldExist.  Any put contexts
  // created after couldExist api returns does not matter, since the put
  // token is invalidated before all of this begins.
  if (config_.enableFastNegativeLookups && !putContexts_[shard].hasContexts() &&
      !navyCache_->couldExist(hk)) {
    stats().numNvmSkippedDeletes.inc();
    return;
  }
  auto& delContexts = delContexts_[shard];
  auto& ctx = delContexts.createContext(hk.key(), std::move(tracker),
                                        std::move(tombstone));

  // capture array reference for delContext. it is stable
  auto delCleanup = [&delContexts, &ctx, this](navy::Status status,
                                               HashedKey) mutable {
    if (auto eventTracker = CacheAPIWrapperForNvm<C>::getEventTracker(cache_)) {
      const auto result = status == navy::Status::Ok
                              ? AllocatorApiResult::REMOVED
                              : (status == navy::Status::NotFound
                                     ? AllocatorApiResult::NOT_FOUND
                                     : AllocatorApiResult::FAILED);
      eventTracker->record(AllocatorApiEvent::NVM_REMOVE, ctx.key(), result);
    }
    delContexts.destroyContext(ctx);
    if (status == navy::Status::Ok || status == navy::Status::NotFound) {
      return;
    }
    // we set disable navy since we failed to delete something
    disableNavy(folly::sformat("Delete Failure. status = {}",
                               static_cast<int>(status)));
  };

  navyCache_->removeAsync(HashedKey::precomputed(ctx.key(), hk.keyHash()),
                          delCleanup);
}

template <typename C>
typename NvmCache<C>::SampleItem NvmCache<C>::getSampleItem() {
  navy::Buffer value;
  auto [status, keyStr] = navyCache_->getRandomAlloc(value);
  if (status != navy::Status::Ok) {
    return SampleItem{true /* fromNvm */};
  }

  folly::StringPiece key(keyStr);

  const auto& nvmItem = *reinterpret_cast<const NvmItem*>(value.data());
  const auto requiredSize =
      Item::getRequiredSize(key, nvmItem.getBlob(0).origAllocSize);

  const auto poolId = nvmItem.poolId();
  auto& pool = cache_.getPool(poolId);
  auto clsId = pool.getAllocationClassId(requiredSize);
  auto allocSize = pool.getAllocationClass(clsId).getAllocSize();

  std::shared_ptr<folly::IOBuf> iobufs =
      createItemAsIOBuf(key, nvmItem, true /* parentOnly */);
  if (!iobufs) {
    return SampleItem{true /* fromNvm */};
  }

  return SampleItem(std::move(*iobufs), poolId, clsId, allocSize,
                    true /* fromNvm */);
}

template <typename C>
bool NvmCache<C>::shutDown() {
  navyEnabled_ = false;
  try {
    this->flushPendingOps();
    navyCache_->persist();
  } catch (const std::exception& e) {
    XLOG(ERR) << "Got error persisting cache: " << e.what();
    return false;
  }
  XLOG(INFO) << "Cache recovery saved to the Flash Device";
  return true;
}

template <typename C>
void NvmCache<C>::flushPendingOps() {
  navyCache_->flush();
}

template <typename C>
util::StatsMap NvmCache<C>::getStatsMap() const {
  util::StatsMap statsMap;
  navyCache_->getCounters(statsMap.createCountVisitor());
  statsMap.insertCount("items_tracked_for_destructor", getNvmItemRemovedSize());
  return statsMap;
}

template <typename C>
void NvmCache<C>::markNvmItemRemovedLocked(HashedKey hk) {
  if (itemDestructor_) {
    itemRemoved_[getShardForKey(hk)].insert(hk.key());
  }
}

template <typename C>
bool NvmCache<C>::checkAndUnmarkItemRemovedLocked(HashedKey hk) {
  auto& removedSet = itemRemoved_[getShardForKey(hk)];
  auto it = removedSet.find(hk.key());
  if (it != removedSet.end()) {
    removedSet.erase(it);
    return true;
  }
  return false;
}

template <typename C>
uint64_t NvmCache<C>::getNvmItemRemovedSize() const {
  uint64_t size = 0;
  for (size_t i = 0; i < kShards; ++i) {
    auto lock = std::unique_lock<std::mutex>{itemDestructorMutex_[i]};
    size += itemRemoved_[i].size();
  }
  return size;
}

} // namespace cachelib
} // namespace facebook

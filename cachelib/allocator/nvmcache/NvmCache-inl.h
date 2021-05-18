namespace facebook {
namespace cachelib {

template <typename C>
std::map<std::string, std::string> NvmCache<C>::Config::serialize() const {
  std::map<std::string, std::string> configMap;
  configMap = navyConfig.serialize();
  configMap["encodeCB"] = encodeCb ? "set" : "empty";
  configMap["decodeCb"] = decodeCb ? "set" : "empty";
  configMap["memoryInsertCb"] = memoryInsertCb ? "set" : "empty";
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
    auto bigHashSizePct = navyConfig.getBigHashSizePct();
    if (bigHashSizePct > 0) {
      auto bucketSize = navyConfig.getBigHashBucketSize();
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
    folly::StringPiece key) {
  const size_t hash = folly::Hash()(key);
  // lower bits for shard and higher bits for key.
  const auto shard = hash % kShards;
  auto guard = tombstones_[shard].add(hash);

  {
    // need to synchronize tombstone creations with fill lock to serialize
    // async fills with deletes
    auto lock = getFillLockForShard(shard);
    cancelFillLocked(key, shard);
  }
  return guard;
}

template <typename C>
bool NvmCache<C>::hasTombStone(folly::StringPiece key) {
  const size_t hash = folly::Hash()(key);
  // lower bits for shard and higher bits for key.
  const auto shard = hash % kShards;
  return tombstones_[shard].isPresent(hash);
}

template <typename C>
typename NvmCache<C>::ItemHandle NvmCache<C>::find(folly::StringPiece key) {
  if (!isEnabled()) {
    return ItemHandle{};
  }

  util::LatencyTracker tracker(stats().nvmLookupLatency_);

  auto shard = getShardForKey(key);
  // invalidateToken any inflight puts for the same key since we are filling
  // from nvmcache.
  inflightPuts_[shard].invalidateToken(key);

  stats().numNvmGets.inc();

  GetCtx* ctx{nullptr};
  ItemHandle hdl{nullptr};
  {
    auto lock = getFillLockForShard(shard);
    // do not use the Cache::find() since that will call back into us.
    hdl = CacheAPIWrapperForNvm<C>::findInternal(cache_, key);
    if (UNLIKELY(hdl != nullptr)) {
      if (hdl->isExpired()) {
        hdl.reset();
        hdl.markExpired();
      }
      return hdl;
    }

    auto& fillMap = getFillMapForShard(shard);
    auto it = fillMap.find(key);
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
    if (config_.enableFastNegativeLookups && it == fillMap.end() &&
        !putContexts_[shard].hasContexts() &&
        !navyCache_->couldExist(makeBufferView(key))) {
      stats().numNvmGetMiss.inc();
      stats().numNvmGetMissFast.inc();
      return ItemHandle{};
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
    auto newCtx = std::make_unique<GetCtx>(*this, key, std::move(waitContext),
                                           std::move(tracker));
    auto res =
        fillMap.emplace(std::make_pair(newCtx->getKey(), std::move(newCtx)));
    XDCHECK(res.second);
    ctx = res.first->second.get();
  } // scope for fill lock

  XDCHECK(ctx);
  auto guard = folly::makeGuard([ctx, this]() { removeFromFillMap(*ctx); });

  auto status = navyCache_->lookupAsync(
      makeBufferView(ctx->getKey()),
      [this, ctx](navy::Status s, navy::BufferView k, navy::Buffer v) {
        this->onGetComplete(*ctx, s, k, v.view());
      });
  if (status != navy::Status::Ok) {
    // instead of disabling navy, we enqueue a delete and return a miss.
    remove(key);
    stats().numNvmGetMiss.inc();
  } else {
    guard.dismiss();
  }

  return hdl;
}

template <typename C>
typename NvmCache<C>::ItemHandle NvmCache<C>::peek(folly::StringPiece key) {
  if (!isEnabled()) {
    return nullptr;
  }

  folly::Baton b;
  ItemHandle hdl{};
  hdl.markWentToNvm();

  // no need for fill lock or inspecting the state of other concurrent
  // operations since we only want to check the state for debugging purposes.
  auto status = navyCache_->lookupAsync(
      makeBufferView(key),
      [&, this](navy::Status st, navy::BufferView, navy::Buffer v) {
        if (st != navy::Status::NotFound) {
          auto dItem = reinterpret_cast<const DipperItem*>(v.data());
          hdl = createItem(key, *dItem);
        }
        b.post();
      });
  if (status != navy::Status::Ok) {
    return hdl;
  }
  b.wait();
  return hdl;
}

template <typename C>
void NvmCache<C>::evictCB(navy::BufferView key,
                          navy::BufferView value,
                          navy::DestructorEvent event) {
  if (event != cachelib::navy::DestructorEvent::Recycled) {
    return;
  }

  stats().numNvmEvictions.inc();

  const auto& dItem = *reinterpret_cast<const DipperItem*>(value.data());
  const auto timeNow = util::getCurrentTimeSec();
  const auto lifetime = timeNow - dItem.getCreationTime();
  const auto expiryTime = dItem.getExpiryTime();
  if (expiryTime != 0) {
    if (expiryTime < timeNow) {
      stats().numNvmExpiredEvict.inc();
      stats().nvmEvictionSecondsPastExpiry_.trackValue(timeNow - expiryTime);
    } else {
      stats().nvmEvictionSecondsToExpiry_.trackValue(expiryTime - timeNow);
    }
  }

  value.size() > navySmallItemThreshold_
      ? stats().nvmLargeLifetimeSecs_.trackValue(lifetime)
      : stats().nvmSmallLifetimeSecs_.trackValue(lifetime);

  ItemHandle hdl;
  try {
    hdl = cache_.peek(folly::StringPiece{
        reinterpret_cast<const char*>(key.data()), key.size()});
  } catch (const exception::RefcountOverflow& ex) {
    XLOGF(ERR,
          "Refcount overflowed when trying peek at an item in "
          "NvmCache::evictCB. key: {}, ex: {}",
          folly::StringPiece{reinterpret_cast<const char*>(key.data()),
                             key.size()},
          ex.what());
  }

  if (!hdl) {
    return;
  }

  if (!hdl->isNvmClean()) {
    // this is a bug
    stats().numNvmUncleanEvict.inc();
  } else {
    if (hdl->isNvmEvicted()) {
      // this means we evicted something twice. This is possible since we
      // could have two copies in the nvm cache and issued the call backs
      // late. Not a correctness issue.
      stats().numNvmCleanDoubleEvict.inc();
    } else {
      hdl->markNvmEvicted();
      stats().numNvmCleanEvict.inc();
    }
  }
}

template <typename C>
NvmCache<C>::NvmCache(C& c, Config config, bool truncate)
    : config_(config.validateAndSetDefaults()),
      cache_(c),
      navySmallItemThreshold_{config_.navyConfig.getBigHashSmallItemMaxSize()} {
  navyCache_ = createNavyCache(
      config_.navyConfig,
      [this](navy::BufferView k, navy::BufferView v, navy::DestructorEvent e) {
        this->evictCB(k, v, e);
      },
      truncate,
      std::move(config.deviceEncryptor));
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
std::unique_ptr<DipperItem> NvmCache<C>::makeDipperItem(const ItemHandle& hdl) {
  const auto& item = *hdl;
  auto poolId = cache_.getAllocInfo((void*)(&item)).poolId;

  if (item.isChainedItem()) {
    throw std::invalid_argument(folly::sformat(
        "Chained item can not be flushed separately {}", hdl->toString()));
  }

  auto chainedItemRange =
      CacheAPIWrapperForNvm<C>::viewAsChainedAllocsRange(cache_, *hdl);
  if (config_.encodeCb &&
      !config_.encodeCb(EncodeDecodeContext{*hdl, chainedItemRange})) {
    return nullptr;
  }

  if (item.hasChainedItem()) {
    std::vector<Blob> blobs;
    blobs.push_back(makeBlob(item));

    for (auto& chainedItem : chainedItemRange) {
      blobs.push_back(makeBlob(chainedItem));
    }

    const size_t bufSize = DipperItem::estimateVariableSize(blobs);
    return std::unique_ptr<DipperItem>(new (bufSize) DipperItem(
        poolId, item.getCreationTime(), item.getExpiryTime(), blobs));
  } else {
    Blob blob = makeBlob(item);
    const size_t bufSize = DipperItem::estimateVariableSize(blob);
    return std::unique_ptr<DipperItem>(new (bufSize) DipperItem(
        poolId, item.getCreationTime(), item.getExpiryTime(), blob));
  }
}

template <typename C>
void NvmCache<C>::put(const ItemHandle& hdl, PutToken token) {
  util::LatencyTracker tracker(stats().nvmInsertLatency_);

  XDCHECK(hdl);
  const auto& item = *hdl;
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
  if (hasTombStone(item.getKey())) {
    stats().numNvmAbortedPutOnTombstone.inc();
    return;
  }

  auto dItem = makeDipperItem(hdl);
  if (!dItem) {
    stats().numNvmPutEncodeFailure.inc();
    return;
  }

  if (item.isNvmClean() && item.isNvmEvicted()) {
    stats().numNvmPutFromClean.inc();
  }

  if (item.isUnevictable()) {
    stats().numNvmPermItems.inc();
  }

  auto iobuf = toIOBuf(std::move(dItem));
  const auto valSize = iobuf.length();
  auto val = folly::ByteRange{iobuf.data(), iobuf.length()};

  auto shard = getShardForKey(item.getKey());
  // obtain the fill lock to record the put context so that any subsequent
  // fill can use this to abandon synchronous negative lookups if enabled.
  auto lock = getFillLockForShard(shard);
  auto& putContexts = putContexts_[shard];
  auto& ctx = putContexts.createContext(item.getKey(), std::move(iobuf),
                                        std::move(tracker));
  lock.unlock(); // once put ctx is instantiated, we don't need the fill lock.
  // capture array reference for putContext. it is stable
  auto putCleanup = [&putContexts, &ctx]() { putContexts.destroyContext(ctx); };
  auto guard = folly::makeGuard([putCleanup]() { putCleanup(); });

  // On a concurrent get, we remove the key from inflight evictions and hence
  // key not being present means a concurrent get happened with an inflight
  // eviction, and we should abandon this write to navy since we already
  // reported the key doesn't exist in the cache.
  const bool executed = token.executeIfValid([&]() {
    auto status = navyCache_->insertAsync(
        makeBufferView(ctx.key()), makeBufferView(val),
        [this, putCleanup, valSize](navy::Status st, navy::BufferView) {
          putCleanup();
          if (st == navy::Status::Ok) {
            stats().nvmPutSize_.trackValue(valSize);
          }
        });

    if (status == navy::Status::Ok) {
      guard.dismiss();
    } else {
      stats().numNvmPutErrs.inc();
    }
  });

  if (!executed) {
    stats().numNvmAbortedPutOnInflightGet.inc();
  }
}

template <typename C>
typename NvmCache<C>::PutToken NvmCache<C>::createPutToken(
    folly::StringPiece key) {
  const auto shard = getShardForKey(key);

  // if there is a concurrent get in flight, then it is possible that it
  // started before the item was visible in the cache. ie the RAM was empty
  // and while the get was in-flight to nvmcache, we inserted something in RAM
  // and are evicting it. See D7861709 for an example race.
  if (mightHaveConcurrentFill(shard, key)) {
    return PutToken{};
  }

  return inflightPuts_[shard].tryAcquireToken(key);
}

template <typename C>
bool NvmCache<C>::mightHaveConcurrentFill(size_t shard,
                                          folly::StringPiece key) {
  XDCHECK_EQ(shard, getShardForKey(key));
  std::unique_lock<std::mutex> l(fillLock_[shard].fillLock_, std::try_to_lock);
  if (!l.owns_lock()) {
    return true;
  }

  const auto& map = getFillMapForShard(shard);
  const bool found = map.find(key) != map.end();
  l.unlock();

  if (found) {
    stats().numNvmAbortedPutOnInflightGet.inc();
  }
  return found;
}

template <typename C>
void NvmCache<C>::onGetComplete(GetCtx& ctx,
                                navy::Status status,
                                navy::BufferView k,
                                navy::BufferView val) {
  auto key =
      folly::StringPiece{reinterpret_cast<const char*>(k.data()), k.size()};
  auto guard = folly::makeGuard([&ctx]() { ctx.cache.removeFromFillMap(ctx); });
  // navy got disabled while we were fetching. If so, safely return a miss.
  // If navy gets disabled beyond this point, it is okay since we fetched it
  // before we got disabled.
  if (!isEnabled()) {
    return;
  }

  if (status != navy::Status::Ok) {
    // instead of disabling navy, we enqueue a delete and return a miss.
    if (status != navy::Status::NotFound) {
      remove(key);
    }
    stats().numNvmGetMiss.inc();
    return;
  }

  if (hasTombStone(key)) {
    stats().numNvmGetMiss.inc();
    return;
  }

  const DipperItem* dItem = reinterpret_cast<const DipperItem*>(val.data());

  // this item expired. return a miss.
  if (dItem->isExpired()) {
    stats().numNvmGetMiss.inc();
    ItemHandle hdl{};
    hdl.markExpired();
    hdl.markWentToNvm();
    ctx.setItemHandle(std::move(hdl));
    return;
  }

  auto it = createItem(key, *dItem);
  if (!it) {
    stats().numNvmGetMiss.inc();
    // we failed to fill due to an internal failure. Return a miss and
    // invalidate what we have in nvmcache
    remove(key);
    return;
  }

  XDCHECK(it->isNvmClean());

  auto lock = getFillLock(key);
  if (ctx.shouldCancelFill()) {
    // a racing remove while we were filling
    return;
  }

  // by the time we filled from navy, another thread inserted in RAM. We
  // disregard.
  if (CacheAPIWrapperForNvm<C>::insertFromNvm(cache_, it)) {
    if (config_.memoryInsertCb) {
      config_.memoryInsertCb(*it);
    }
    it.markWentToNvm();
    ctx.setItemHandle(std::move(it));
  }
} // namespace cachelib

template <typename C>
typename NvmCache<C>::ItemHandle NvmCache<C>::createItem(
    folly::StringPiece key, const DipperItem& dItem) {
  const size_t numBufs = dItem.getNumBlobs();
  // parent item
  XDCHECK_GE(numBufs, 1u);
  const auto pBlob = dItem.getBlob(0);

  stats().numNvmAllocAttempts.inc();
  // use the original alloc size to allocate, but make sure that the usable
  // size matches the pBlob's size
  auto it = CacheAPIWrapperForNvm<C>::allocateInternal(
      cache_, dItem.poolId(), key, pBlob.origAllocSize, dItem.getCreationTime(),
      dItem.getExpiryTime(), false);
  if (!it) {
    return nullptr;
  }

  XDCHECK_LE(pBlob.data.size(), getStorageSizeInNvm(*it));
  XDCHECK_LE(pBlob.origAllocSize, pBlob.data.size());
  ::memcpy(it->getWritableMemory(), pBlob.data.data(), pBlob.data.size());
  it->markNvmClean();

  // if we have more, then we need to allocate them as chained items and add
  // them in the same order. To do that, we need to add them from the inverse
  // order
  if (numBufs > 1) {
    // chained items need to be added in reverse order to maintain the same
    // order as what we serialized.
    for (int i = numBufs - 1; i >= 1; i--) {
      auto cBlob = dItem.getBlob(i);
      XDCHECK_GT(cBlob.origAllocSize, 0u);
      XDCHECK_GT(cBlob.data.size(), 0u);
      stats().numNvmAllocAttempts.inc();
      auto chainedIt = cache_.allocateChainedItem(it, cBlob.origAllocSize);
      if (!chainedIt) {
        return nullptr;
      }
      XDCHECK(chainedIt->isChainedItem());
      XDCHECK_LE(cBlob.data.size(), getStorageSizeInNvm(*chainedIt));
      ::memcpy(chainedIt->getWritableMemory(), cBlob.data.data(),
               cBlob.data.size());
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
void NvmCache<C>::disableNavy(const std::string& msg) {
  if (isEnabled()) {
    navyEnabled_ = false;
    XLOGF(CRITICAL, "Disabling navy. {}", msg);
  }
}

template <typename C>
void NvmCache<C>::cancelFillLocked(folly::StringPiece key, size_t shard) {
  auto& map = getFillMapForShard(shard);
  auto it = map.find(key);
  if (it != map.end()) {
    it->second->cancelFill();
  }
}

template <typename C>
void NvmCache<C>::remove(folly::StringPiece key) {
  if (!isEnabled()) {
    return;
  }

  util::LatencyTracker tracker(stats().nvmRemoveLatency_);
  const auto shard = getShardForKey(key);
  auto& delContexts = delContexts_[shard];
  auto& ctx = delContexts.createContext(key, std::move(tracker));

  // capture array reference for delContext. it is stable
  auto delCleanup = [&delContexts, &ctx, this](navy::Status status,
                                               navy::BufferView) {
    delContexts.destroyContext(ctx);
    if (status == navy::Status::Ok || status == navy::Status::NotFound) {
      return;
    }
    // we set disable navy since we failed to delete something
    disableNavy(folly::sformat("Delete Failure. status = {}",
                               static_cast<int>(status)));
  };

  stats().numNvmDeletes.inc();

  // invalidate any inflight put that is on flight since we are queueing up a
  // deletion.
  inflightPuts_[shard].invalidateToken(key);

  auto lock = getFillLockForShard(shard);
  cancelFillLocked(key, shard);

  auto status = navyCache_->removeAsync(makeBufferView(ctx.key()), delCleanup);
  if (status != navy::Status::Ok) {
    delCleanup(status, {});
  }
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
std::unordered_map<std::string, double> NvmCache<C>::getStatsMap() const {
  std::unordered_map<std::string, double> statsMap;
  navyCache_->getCounters([&statsMap](folly::StringPiece key, double value) {
    auto keyStr = key.str();
    DCHECK_EQ(0, statsMap.count(keyStr));
    statsMap.insert({std::move(keyStr), value});
  });
  return statsMap;
}

} // namespace cachelib
} // namespace facebook

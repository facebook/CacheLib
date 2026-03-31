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

#include "cachelib/interface/components/RAMCacheComponent.h"

#include <folly/logging/xlog.h>

#include "cachelib/allocator/CacheAllocator.h"

namespace facebook::cachelib::interface {

using LruCacheItem = cachelib::CacheItem<LruCacheTrait>;

/**
 * Wrapper around CacheItem<LruCacheTrait> (aka LruCacheItem) for use by
 * RAMCacheComponent. RAMCacheItem is embedded inside LruCacheItem so that it's
 * maintained automatically in cache. Note that LruCacheItem is oblivious to
 * RAMCacheItem, it's considered part of value memory. The item's layout is:
 *
 *   -------------------------------------------------
 *   | header |   key   || RAMCacheItem |   value   ||
 *   -------------------------------------------------
 *           ( double bars represent LruCacheItem's value memory)
 *
 * RAMCacheItem maintains two pointers - an implicit vtable ref and a pointer to
 * the containing LruCacheItem for access to standard APIs.  RAMCacheItem
 * accounts for itself in everything exposed to the user.
 *
 * Note: RAMCacheItem needs to keep the pointer to the containing LruCacheItem
 * because we can't automatically calculate the address of the containing item
 * due to the variable-sized key.
 *
 * TODO(rlyerly) remove this shim and have LruCacheItem directly implement
 * interface::CacheItem
 */
class RAMCacheItem : public interface::CacheItem {
 public:
  /**
   * Initialize a RAMCacheItem embedded in an already-allocated LruCacheItem.
   * @param item the allocated LruCacheItem
   * @return pointer to the initialized RAMCacheItem
   */
  static RAMCacheItem* init(LruCacheItem* item) {
    auto* embeddedItem = item->getMemoryAs<RAMCacheItem>();
    new (embeddedItem) RAMCacheItem(item);
    return embeddedItem;
  }

  /**
   * Get the containing LruCacheItem.
   * @return the containing cache item
   */
  LruCacheItem* item() const noexcept { return item_; }

  // ------------------------------ Interface ------------------------------ //

  uint32_t getCreationTime() const noexcept override {
    return item_->getCreationTime();
  }
  uint32_t getExpiryTime() const noexcept override {
    return item_->getExpiryTime();
  }
  // TODO return error result if this fails
  void incrementRefCount() noexcept override { item_->incRef(); }
  bool decrementRefCount() noexcept override { return item_->decRef() == 0; }
  Key getKey() const noexcept override { return item_->getKey(); }
  void* getMemory() const noexcept override {
    return static_cast<char*>(item_->getMemory()) + sizeof(RAMCacheItem);
  }
  uint32_t getMemorySize() const noexcept override {
    return item_->getSize() - sizeof(RAMCacheItem);
  }
  uint32_t getTotalSize() const noexcept override {
    return item_->getTotalSize();
  }

 private:
  LruCacheItem* item_;

  explicit RAMCacheItem(LruCacheItem* item) : item_(item) {}
};
// RAMCacheItem should only contain vtable and LruCacheItem pointers
static_assert(sizeof(RAMCacheItem) == (2 * sizeof(void*)));

namespace {

std::unique_ptr<LruAllocator> getCache(
    LruAllocatorConfig&& config,
    const RAMCacheComponent::PersistenceConfig& persistenceConfig) {
  if (persistenceConfig.recover()) {
    try {
      return std::make_unique<LruAllocator>(LruAllocator::SharedMemAttach,
                                            folly::copy(config));
    } catch (const std::exception& e) {
      XLOG(WARN) << "Failed to recover RAM cache from shared memory ("
                 << e.what() << "), creating a new empty cache";
      return std::make_unique<LruAllocator>(LruAllocator::SharedMemNew,
                                            std::move(config));
    }
  } else if (persistenceConfig.persist()) {
    return std::make_unique<LruAllocator>(LruAllocator::SharedMemNew,
                                          std::move(config));
  } else {
    return std::make_unique<LruAllocator>(std::move(config));
  }
}

template <typename HandleT>
LruCacheItem* getImplItemFromHandle(HandleT& handle) {
  return reinterpret_cast<const RAMCacheItem*>(handle.get())->item();
}

template <typename HandleT, typename ImplHandleT>
HandleT toGenericHandle(RAMCacheComponent& cache,
                        const ImplHandleT& implHandle) {
  auto* implItem = const_cast<LruCacheItem*>(implHandle.get());
  auto* embeddedItem = RAMCacheItem::init(implItem);
  return HandleT(cache, *embeddedItem);
}

} // namespace

/* static */ RAMCacheComponent::PersistenceConfig
RAMCacheComponent::PersistenceConfig::noPersistenceOrRecovery() {
  return PersistenceConfig(/* persist */ false, /* recover */ false,
                           /* cacheDir */ std::string(),
                           /* baseAddr */ nullptr);
}

/* static */ RAMCacheComponent::PersistenceConfig
RAMCacheComponent::PersistenceConfig::persistenceAndRecovery(
    std::string cacheDir, void* baseAddr) {
  return PersistenceConfig(/* persist */ true, /* recover */ true,
                           std::move(cacheDir), baseAddr);
}

/* static */ RAMCacheComponent::PersistenceConfig
RAMCacheComponent::PersistenceConfig::persistenceButNoRecovery(
    std::string cacheDir, void* baseAddr) {
  return PersistenceConfig(/* persist */ true, /* recover */ false,
                           std::move(cacheDir), baseAddr);
}

/* static */ Result<RAMCacheComponent> RAMCacheComponent::create(
    LruAllocatorConfig&& allocConfig,
    PoolConfig&& poolConfig,
    PersistenceConfig persistenceConfig,
    const LatencySamplingConfig& latencySamplingConfig) noexcept {
  if (allocConfig.nvmConfig) {
    return makeError(Error::Code::INVALID_CONFIG,
                     "RAMCacheComponent does not support NVM cache");
  } else if (allocConfig.poolRebalancingEnabled()) {
    return makeError(Error::Code::INVALID_CONFIG,
                     "RAMCacheComponent does not support pool rebalancing");
  } else if (persistenceConfig.persist() || persistenceConfig.recover()) {
    if (persistenceConfig.cacheDir().empty()) {
      return makeError(Error::Code::INVALID_CONFIG,
                       "cacheDir must be set for persistence/recovery");
    }
    allocConfig.enableCachePersistence(std::move(persistenceConfig.cacheDir()),
                                       persistenceConfig.baseAddr());
  }
  try {
    auto cache = RAMCacheComponent(std::move(allocConfig), persistenceConfig,
                                   latencySamplingConfig);
    auto ids = cache.cache_->getPoolIds();
    if (!ids.empty()) {
      // Pool was restored from shared memory, just look up its ID
      XCHECK_EQ(ids.size(), 1UL);
      cache.defaultPool_ = *ids.begin();
    } else {
      // Add a default pool that will be used for all allocations
      cache.defaultPool_ = cache.cache_->addPool(
          poolConfig.name_, poolConfig.size_, poolConfig.allocSizes_,
          poolConfig.mmConfig_, poolConfig.rebalanceStrategy_,
          poolConfig.resizeStrategy_, poolConfig.ensureProvisionable_);
    }
    return cache;
  } catch (const std::exception& e) {
    return makeError(Error::Code::INVALID_CONFIG, e.what());
  }
}

LruAllocator& RAMCacheComponent::get() const noexcept { return *cache_; }

const std::string& RAMCacheComponent::getName() const noexcept {
  return cache_->config_.getCacheName();
}

folly::coro::Task<Result<AllocatedHandle>> RAMCacheComponent::allocate(
    Key key, uint32_t size, uint32_t creationTime, uint32_t ttlSecs) {
  stats_->allocate_.throughput_.calls_.inc();
  // Latency is not tracked here; CacheAllocator already tracks it via
  // stats_.allocateLatency_. See getStats().

  try {
    auto implHandle = cache_->allocate(
        defaultPool_, key, size + sizeof(RAMCacheItem), ttlSecs, creationTime);
    if (!implHandle) {
      stats_->allocate_.throughput_.errors_.inc();
      co_return makeError(
          Error::Code::NO_SPACE,
          fmt::format("could not find room in cache for {}", key));
    }
    stats_->allocate_.throughput_.successes_.inc();
    co_return toGenericHandle<AllocatedHandle>(*this, implHandle);
  } catch (const std::exception& e) {
    stats_->allocate_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INVALID_ARGUMENTS, e.what());
  }
}

folly::coro::Task<UnitResult> RAMCacheComponent::insert(
    AllocatedHandle&& handle) {
  stats_->insert_.throughput_.calls_.inc();

  if (!handle) {
    stats_->insert_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "empty AllocatedHandle");
  }

  auto latencyGuard = stats_->insert_.latency_.start();
  try {
    auto* implItem = getImplItemFromHandle(handle);
    auto implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that hasn't been inserted";
      stats_->insert_.throughput_.errors_.inc();
      co_return makeError(Error::Code::INSERT_FAILED, "item is being evicted");
    }
    if (!cache_->insert(implHandle)) {
      stats_->insert_.throughput_.errors_.inc();
      co_return makeError(Error::Code::ALREADY_INSERTED,
                          "key already exists in cache");
    }
  } catch (const std::invalid_argument& ia) {
    // Should only happen when implHandle->isAccessible() == true
    XLOG(DFATAL) << "Double-insert from the same allocated handle - did we "
                    "forget to release the AllocatedHandle after inserting?";
    stats_->insert_.throughput_.errors_.inc();
    co_return makeError(Error::Code::ALREADY_INSERTED, ia.what());
  } catch (const std::exception& e) {
    stats_->insert_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INSERT_FAILED, e.what());
  }

  stats_->insert_.throughput_.successes_.inc();
  auto _ = std::move(handle);
  co_return folly::unit;
}

folly::coro::Task<Result<std::optional<AllocatedHandle>>>
RAMCacheComponent::insertOrReplace(AllocatedHandle&& handle) {
  stats_->insertOrReplace_.throughput_.calls_.inc();

  if (!handle) {
    stats_->insertOrReplace_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "empty AllocatedHandle");
  }

  auto latencyGuard = stats_->insertOrReplace_.latency_.start();
  LruAllocator::WriteHandle replacedHandle;
  try {
    auto* implItem = getImplItemFromHandle(handle);
    auto implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that hasn't been inserted";
      stats_->insertOrReplace_.throughput_.errors_.inc();
      co_return makeError(Error::Code::INSERT_FAILED, "item is being evicted");
    }
    replacedHandle = cache_->insertOrReplace(implHandle);
  } catch (const std::invalid_argument& ia) {
    // Should only happen when implHandle->isAccessible() == true
    XLOG(DFATAL) << "Double-insert from the same allocated handle - did we "
                    "forget to release the AllocatedHandle after inserting?";
    stats_->insertOrReplace_.throughput_.errors_.inc();
    co_return makeError(Error::Code::ALREADY_INSERTED, ia.what());
  } catch (const std::exception& e) {
    stats_->insertOrReplace_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INSERT_FAILED, e.what());
  }

  stats_->insertOrReplace_.throughput_.successes_.inc();
  auto _ = std::move(handle);
  if (replacedHandle) {
    co_return toGenericHandle<AllocatedHandle>(*this, replacedHandle);
  } else {
    co_return std::nullopt;
  }
}

folly::coro::Task<Result<std::optional<ReadHandle>>> RAMCacheComponent::find(
    Key key) {
  stats_->find_.throughput_.calls_.inc();
  auto latencyGuard = stats_->find_.latency_.start();

  if (auto handle = cache_->find(key)) {
    stats_->find_.throughput_.hits_.inc();
    stats_->find_.throughput_.successes_.inc();
    co_return toGenericHandle<ReadHandle>(*this, handle);
  }
  stats_->find_.throughput_.misses_.inc();
  stats_->find_.throughput_.successes_.inc();
  co_return std::nullopt;
}

folly::coro::Task<Result<std::optional<WriteHandle>>>
RAMCacheComponent::findToWrite(Key key) {
  stats_->findToWrite_.throughput_.calls_.inc();
  auto latencyGuard = stats_->findToWrite_.latency_.start();

  if (auto handle = cache_->findToWrite(key)) {
    stats_->findToWrite_.throughput_.hits_.inc();
    stats_->findToWrite_.throughput_.successes_.inc();
    co_return toGenericHandle<WriteHandle>(*this, handle);
  }
  stats_->findToWrite_.throughput_.misses_.inc();
  stats_->findToWrite_.throughput_.successes_.inc();
  co_return std::nullopt;
}

folly::coro::Task<Result<bool>> RAMCacheComponent::remove(Key key) {
  // calls_, hits_, and misses_ are not tracked here; CacheAllocator already
  // tracks them via stats_.numCacheRemoves and stats_.numCacheRemoveRamHits.
  // See getStats().
  auto latencyGuard = stats_->removeByKey_.latency_.start();

  bool removed = cache_->remove(key) == LruAllocator::RemoveRes::kSuccess;
  stats_->removeByKey_.throughput_.successes_.inc();
  co_return removed;
}

folly::coro::Task<UnitResult> RAMCacheComponent::remove(ReadHandle&& handle) {
  stats_->removeByHandle_.throughput_.calls_.inc();

  if (!handle) {
    stats_->removeByHandle_.throughput_.errors_.inc();
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "removing empty ReadHandle");
  }

  auto latencyGuard = stats_->removeByHandle_.latency_.start();
  try {
    auto* implItem = getImplItemFromHandle(handle);
    LruAllocator::ReadHandle implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that has an outstanding reference";
      stats_->removeByHandle_.throughput_.errors_.inc();
      co_return makeError(Error::Code::REMOVE_FAILED, "item is being evicted");
    }
    cache_->remove(implHandle);
  } catch (const std::exception& e) {
    stats_->removeByHandle_.throughput_.errors_.inc();
    co_return makeError(Error::Code::REMOVE_FAILED, e.what());
  }

  stats_->removeByHandle_.throughput_.successes_.inc();
  auto _ = std::move(handle);
  co_return folly::unit;
}

RAMCacheComponent::RAMCacheComponent(
    LruAllocatorConfig&& config,
    const PersistenceConfig& persistenceConfig,
    const LatencySamplingConfig& latencySamplingConfig)
    : CacheComponentWithStats(latencySamplingConfig),
      cache_(getCache(std::move(config), persistenceConfig)),
      defaultPool_(Slab::kInvalidPoolId),
      persist_(persistenceConfig.persist()),
      lastStatsCollectionTime_(std::chrono::steady_clock::now()) {}

UnitResult RAMCacheComponent::writeBack(CacheItem& /* item */) {
  stats_->writeBack_.throughput_.calls_.inc();
  /* no-op, writing to the RAM buffer is equivalent to writing back */
  stats_->writeBack_.throughput_.successes_.inc();
  return folly::unit;
}

void RAMCacheComponent::release(interface::CacheItem& item, bool inserted) {
  stats_->release_.throughput_.calls_.inc();
  auto latencyGuard = stats_->release_.latency_.start();
  auto* implItem = reinterpret_cast<RAMCacheItem&>(item).item();
  cache_->releaseBackToAllocator(*implItem, RemoveContext::kNormal,
                                 /* nascent */ !inserted);
  stats_->release_.throughput_.successes_.inc();
}

UnitResult RAMCacheComponent::shutdown() {
  if (persist_) {
    switch (cache_->shutDown()) {
    case LruAllocator::ShutDownStatus::kSuccess: // fall-through
    case LruAllocator::ShutDownStatus::kSavedOnlyDRAM:
      break;
    case LruAllocator::ShutDownStatus::kSavedOnlyNvmCache: // fall-through
    case LruAllocator::ShutDownStatus::kFailed:            // fall-through
    default:
      return makeError(Error::Code::SHUTDOWN_FAILED,
                       "failed to persist RAM cache state");
    }
  } else {
    // shutDown() internally calls stopWorkers() but doesn't check the return
    // value so we won't either
    cache_->stopWorkers();
  }
  return folly::unit;
}

CacheComponentStats RAMCacheComponent::getStats() const noexcept {
  CacheComponentStats stats(*stats_);

  // Populate removeByKey throughput from CacheAllocator's counters
  auto& allocatorStats = cache_->stats();
  stats.removeByKey_.throughput_.calls_ = allocatorStats.numCacheRemoves.get();
  stats.removeByKey_.throughput_.hits_ =
      allocatorStats.numCacheRemoveRamHits.get();
  stats.removeByKey_.throughput_.misses_ =
      stats.removeByKey_.throughput_.calls_ -
      stats.removeByKey_.throughput_.hits_;

  // Populate allocate latency from CacheAllocator's counter
  stats.allocate_.latency_ = allocatorStats.allocateLatency_.estimate();

  auto now = std::chrono::steady_clock::now();
  auto prev = std::exchange(lastStatsCollectionTime_, now);
  cache_->exportStats(
      /* statPrefix */ "",
      std::chrono::duration_cast<std::chrono::seconds>(now - prev),
      [&stats](folly::StringPiece name, uint64_t value) {
        stats.extraStats_.insertCount(name.toString(), value);
      });

  return stats;
}

} // namespace facebook::cachelib::interface

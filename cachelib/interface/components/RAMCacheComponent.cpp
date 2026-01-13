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

/* static */ Result<RAMCacheComponent> RAMCacheComponent::create(
    LruAllocatorConfig&& allocConfig, PoolConfig&& poolConfig) noexcept {
  if (allocConfig.nvmConfig) {
    return makeError(Error::Code::INVALID_CONFIG,
                     "RAMCacheComponent does not support NVM cache");
  } else if (allocConfig.poolRebalancingEnabled()) {
    return makeError(Error::Code::INVALID_CONFIG,
                     "RAMCacheComponent does not support pool rebalancing");
  }
  try {
    auto cache = RAMCacheComponent(std::move(allocConfig));
    // Add a default pool that will be used for all allocations
    cache.defaultPool_ = cache.cache_->addPool(poolConfig.name_,
                                               poolConfig.size_,
                                               poolConfig.allocSizes_,
                                               poolConfig.mmConfig_,
                                               poolConfig.rebalanceStrategy_,
                                               poolConfig.resizeStrategy_,
                                               poolConfig.ensureProvisionable_);
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
  try {
    auto implHandle = cache_->allocate(
        defaultPool_, key, size + sizeof(RAMCacheItem), ttlSecs, creationTime);
    if (!implHandle) {
      co_return makeError(
          Error::Code::NO_SPACE,
          fmt::format("could not find room in cache for {}", key));
    }
    co_return toGenericHandle<AllocatedHandle>(*this, implHandle);
  } catch (const std::exception& e) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS, e.what());
  }
}

folly::coro::Task<UnitResult> RAMCacheComponent::insert(
    AllocatedHandle&& handle) {
  if (!handle) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "empty AllocatedHandle");
  }

  try {
    auto* implItem = getImplItemFromHandle(handle);
    auto implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that hasn't been inserted";
      co_return makeError(Error::Code::INSERT_FAILED, "item is being evicted");
    }
    if (!cache_->insert(implHandle)) {
      co_return makeError(Error::Code::ALREADY_INSERTED,
                          "key already exists in cache");
    }
  } catch (const std::invalid_argument& ia) {
    // Should only happen when implHandle->isAccessible() == true
    XLOG(DFATAL) << "Double-insert from the same allocated handle - did we "
                    "forget to release the AllocatedHandle after inserting?";
    co_return makeError(Error::Code::ALREADY_INSERTED, ia.what());
  } catch (const std::exception& e) {
    co_return makeError(Error::Code::INSERT_FAILED, e.what());
  }

  auto _ = std::move(handle);
  co_return folly::unit;
}

folly::coro::Task<Result<std::optional<AllocatedHandle>>>
RAMCacheComponent::insertOrReplace(AllocatedHandle&& handle) {
  if (!handle) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "empty AllocatedHandle");
  }

  LruAllocator::WriteHandle replacedHandle;
  try {
    auto* implItem = getImplItemFromHandle(handle);
    auto implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that hasn't been inserted";
      co_return makeError(Error::Code::INSERT_FAILED, "item is being evicted");
    }
    replacedHandle = cache_->insertOrReplace(implHandle);
  } catch (const std::invalid_argument& ia) {
    // Should only happen when implHandle->isAccessible() == true
    XLOG(DFATAL) << "Double-insert from the same allocated handle - did we "
                    "forget to release the AllocatedHandle after inserting?";
    co_return makeError(Error::Code::ALREADY_INSERTED, ia.what());
  } catch (const std::exception& e) {
    co_return makeError(Error::Code::INSERT_FAILED, e.what());
  }

  auto _ = std::move(handle);
  if (replacedHandle) {
    co_return toGenericHandle<AllocatedHandle>(*this, replacedHandle);
  } else {
    co_return std::nullopt;
  }
}

folly::coro::Task<Result<std::optional<ReadHandle>>> RAMCacheComponent::find(
    Key key) {
  if (auto handle = cache_->find(key)) {
    co_return toGenericHandle<ReadHandle>(*this, handle);
  }
  co_return std::nullopt;
}

folly::coro::Task<Result<std::optional<WriteHandle>>>
RAMCacheComponent::findToWrite(Key key) {
  if (auto handle = cache_->findToWrite(key)) {
    co_return toGenericHandle<WriteHandle>(*this, handle);
  }
  co_return std::nullopt;
}

folly::coro::Task<Result<bool>> RAMCacheComponent::remove(Key key) {
  co_return cache_->remove(key) == LruAllocator::RemoveRes::kSuccess;
}

folly::coro::Task<UnitResult> RAMCacheComponent::remove(ReadHandle&& handle) {
  if (!handle) {
    co_return makeError(Error::Code::INVALID_ARGUMENTS,
                        "removing empty ReadHandle");
  }

  try {
    auto* implItem = getImplItemFromHandle(handle);
    LruAllocator::ReadHandle implHandle = cache_->acquire(implItem);
    if (!implHandle) {
      XLOG(DFATAL) << "Evicting item that has an outstanding reference";
      co_return makeError(Error::Code::REMOVE_FAILED, "item is being evicted");
    }
    cache_->remove(implHandle);
  } catch (const std::exception& e) {
    co_return makeError(Error::Code::REMOVE_FAILED, e.what());
  }

  auto _ = std::move(handle);
  co_return folly::unit;
}

RAMCacheComponent::RAMCacheComponent(LruAllocatorConfig&& config)
    : cache_(std::make_unique<LruAllocator>(std::move(config))) {}

UnitResult RAMCacheComponent::writeBack(CacheItem& /* item */) {
  /* no-op, writing to the RAM buffer is equivalent to writing back */
  return folly::unit;
}

folly::coro::Task<void> RAMCacheComponent::release(interface::CacheItem& item,
                                                   bool inserted) {
  auto* implItem = reinterpret_cast<RAMCacheItem&>(item).item();
  cache_->releaseBackToAllocator(
      *implItem, RemoveContext::kNormal, /* nascent */ !inserted);
  co_return;
}

} // namespace facebook::cachelib::interface

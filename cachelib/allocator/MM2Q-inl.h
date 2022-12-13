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

namespace facebook {
namespace cachelib {

/* Container Interface Implementation */
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::Container(const serialization::MM2QObject& object,
                                       PtrCompressor compressor)
    : lru_(*object.lrus(), compressor),
      tailTrackingEnabled_(*object.tailTrackingEnabled()),
      config_(*object.config()) {
  lruRefreshTime_ = config_.lruRefreshTime;
  nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                             ? std::numeric_limits<Time>::max()
                             : static_cast<Time>(util::getCurrentTimeSec()) +
                                   config_.mmReconfigureIntervalSecs.count();

  // We need to adjust list positions if the previous version does not have
  // tail lists (WarmTail & ColdTail), in order to potentially avoid cold roll
  if (object.lrus()->lists()->size() < LruType::NumTypes) {
    XDCHECK_EQ(false, tailTrackingEnabled_);
    XDCHECK_EQ(object.lrus()->lists()->size() + 2, LruType::NumTypes);
    lru_.insertEmptyListAt(LruType::WarmTail, compressor);
    lru_.insertEmptyListAt(LruType::ColdTail, compressor);
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::recordAccess(T& node,
                                               AccessMode mode) noexcept {
  if ((mode == AccessMode::kWrite && !config_.updateOnWrite) ||
      (mode == AccessMode::kRead && !config_.updateOnRead)) {
    return false;
  }

  const auto curr = static_cast<Time>(util::getCurrentTimeSec());
  // check if the node is still being memory managed
  if (node.isInMMContainer() &&
      ((curr >= getUpdateTime(node) +
                    lruRefreshTime_.load(std::memory_order_relaxed)))) {
    auto func = [&]() {
      reconfigureLocked(curr);
      if (!node.isInMMContainer()) {
        return false;
      }
      if (isHot(node)) {
        lru_.getList(LruType::Hot).moveToHead(node);
        ++numHotAccesses_;
      } else if (isCold(node)) {
        if (inTail(node)) {
          unmarkTail(node);
          lru_.getList(LruType::ColdTail).remove(node);
          ++numColdTailAccesses_;
        } else {
          lru_.getList(LruType::Cold).remove(node);
        }
        lru_.getList(LruType::Warm).linkAtHead(node);
        unmarkCold(node);
        ++numColdAccesses_;
        // only rebalance if config says so. recordAccess is called mostly on
        // latency sensitive cache get operations.
        if (config_.rebalanceOnRecordAccess) {
          rebalance();
        }
      } else {
        if (inTail(node)) {
          unmarkTail(node);
          lru_.getList(LruType::WarmTail).remove(node);
          lru_.getList(LruType::Warm).linkAtHead(node);
          ++numWarmTailAccesses_;
        } else {
          lru_.getList(LruType::Warm).moveToHead(node);
        }
        ++numWarmAccesses_;
      }
      setUpdateTime(node, curr);
      return true;
    };

    // if the tryLockUpdate optimization is on, and we were able to grab the
    // lock, execute the critical section and return true, else return false
    //
    // if the tryLockUpdate optimization is off, we always execute the critical
    // section and return true
    if (config_.tryLockUpdate) {
      if (auto lck = LockHolder{*lruMutex_, std::try_to_lock}) {
        return func();
      }
      return false;
    }

    return lruMutex_->lock_combine(func);
  }
  return false;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStat(
    uint64_t projectedLength) const noexcept {
  return lruMutex_->lock_combine([this, projectedLength]() {
    return getEvictionAgeStatLocked(projectedLength);
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
cachelib::EvictionAgeStat MM2Q::Container<T, HookPtr>::getEvictionAgeStatLocked(
    uint64_t projectedLength) const noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());

  auto getProjectedAge = [this, currTime, projectedLength](
                             auto lruType, auto oldestElementAge) {
    auto it = lru_.rbegin(lruType);
    for (size_t numSeen = 0; numSeen < projectedLength && it != lru_.rend();
         ++numSeen, ++it) {
    }
    return (it != lru_.rend()) ? currTime - getUpdateTime(*it)
                               : oldestElementAge;
  };

  EvictionAgeStat stat;

  stat.hotQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::Hot, currTime);
  stat.hotQueueStat.size = lru_.getList(LruType::Hot).size();
  stat.hotQueueStat.projectedAge =
      getProjectedAge(LruType::Hot, stat.hotQueueStat.oldestElementAge);

  // sum the tail and the main list for the ones that have tail.
  stat.warmQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::WarmTail, currTime);
  stat.warmQueueStat.size = lru_.getList(LruType::Warm).size() +
                            lru_.getList(LruType::WarmTail).size();
  stat.warmQueueStat.projectedAge =
      getProjectedAge(LruType::WarmTail, stat.warmQueueStat.oldestElementAge);

  stat.coldQueueStat.oldestElementAge =
      getOldestAgeLocked(LruType::ColdTail, currTime);
  stat.coldQueueStat.size = lru_.getList(LruType::ColdTail).size() +
                            lru_.getList(LruType::Cold).size();
  stat.coldQueueStat.projectedAge =
      getProjectedAge(LruType::ColdTail, stat.coldQueueStat.oldestElementAge);

  return stat;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
uint32_t MM2Q::Container<T, HookPtr>::getOldestAgeLocked(
    LruType lruType, Time currentTime) const noexcept {
  auto it = lru_.rbegin(lruType);
  return it != lru_.rend() ? currentTime - getUpdateTime(*it) : 0;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::LruType MM2Q::Container<T, HookPtr>::getLruType(
    const T& node) const noexcept {
  if (isHot(node)) {
    return LruType::Hot;
  }
  if (isCold(node)) {
    return inTail(node) ? LruType::ColdTail : LruType::Cold;
  }
  return inTail(node) ? LruType::WarmTail : LruType::Warm;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::rebalance() noexcept {
  // shrink Warm (and WarmTail) if their total size is larger than expected
  size_t expectedSize = config_.getWarmSizePercent() * lru_.size() / 100;
  while (lru_.getList(LruType::Warm).size() +
             lru_.getList(LruType::WarmTail).size() >
         expectedSize) {
    auto popFrom = [&](LruType lruType) -> T* {
      auto& lru = lru_.getList(lruType);
      T* node = lru.getTail();
      XDCHECK(node);
      lru.remove(*node);
      if (lruType == WarmTail || lruType == ColdTail) {
        unmarkTail(*node);
      }
      return node;
    };
    // remove from warm tail if it is not empty. if empty, remove from warm.
    T* node = lru_.getList(LruType::WarmTail).size() > 0
                  ? popFrom(LruType::WarmTail)
                  : popFrom(LruType::Warm);
    XDCHECK(isWarm(*node));
    lru_.getList(LruType::Cold).linkAtHead(*node);
    markCold(*node);
  }

  // shrink Hot if its size is larger than expected
  expectedSize = config_.hotSizePercent * lru_.size() / 100;
  while (lru_.getList(LruType::Hot).size() > expectedSize) {
    auto node = lru_.getList(LruType::Hot).getTail();
    XDCHECK(node);
    XDCHECK(isHot(*node));
    lru_.getList(LruType::Hot).remove(*node);
    lru_.getList(LruType::Cold).linkAtHead(*node);
    unmarkHot(*node);
    markCold(*node);
  }

  // adjust tail sizes for Cold and Warm
  adjustTail(LruType::Cold);
  adjustTail(LruType::Warm);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::add(T& node) noexcept {
  const auto currTime = static_cast<Time>(util::getCurrentTimeSec());
  return lruMutex_->lock_combine([this, &node, currTime]() {
    if (node.isInMMContainer()) {
      return false;
    }

    markHot(node);
    unmarkCold(node);
    unmarkTail(node);
    lru_.getList(LruType::Hot).linkAtHead(node);
    rebalance();

    node.markInMMContainer();
    setUpdateTime(node, currTime);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Container<T, HookPtr>::LockedIterator
MM2Q::Container<T, HookPtr>::getEvictionIterator() const noexcept {
  LockHolder l(*lruMutex_);
  return LockedIterator{std::move(l), lru_.rbegin()};
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
template <typename F>
void MM2Q::Container<T, HookPtr>::withEvictionIterator(F&& fun) {
  if (config_.useCombinedLockForIterators) {
    lruMutex_->lock_combine([this, &fun]() { fun(Iterator{lru_.rbegin()}); });
  } else {
    LockHolder lck{*lruMutex_};
    fun(Iterator{lru_.rbegin()});
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::removeLocked(T& node,
                                               bool doRebalance) noexcept {
  LruType type = getLruType(node);
  lru_.getList(type).remove(node);
  if (doRebalance) {
    rebalance();
  }

  node.unmarkInMMContainer();
  return;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::setConfig(const Config& newConfig) {
  if (!tailTrackingEnabled_ && newConfig.tailSize > 0) {
    throw std::invalid_argument(
        "Cannot turn on tailHitsTracking (cache drop needed)");
  }
  if (tailTrackingEnabled_ && !newConfig.tailSize) {
    throw std::invalid_argument(
        "Cannot turn off tailHitsTracking (cache drop needed)");
  }

  lruMutex_->lock_combine([this, &newConfig]() {
    config_ = newConfig;
    lruRefreshTime_.store(config_.lruRefreshTime, std::memory_order_relaxed);
    nextReconfigureTime_ = config_.mmReconfigureIntervalSecs.count() == 0
                               ? std::numeric_limits<Time>::max()
                               : static_cast<Time>(util::getCurrentTimeSec()) +
                                     config_.mmReconfigureIntervalSecs.count();
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
typename MM2Q::Config MM2Q::Container<T, HookPtr>::getConfig() const {
  return lruMutex_->lock_combine([this]() { return config_; });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::remove(T& node) noexcept {
  return lruMutex_->lock_combine([this, &node]() {
    if (!node.isInMMContainer()) {
      return false;
    }
    removeLocked(node);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::remove(Iterator& it) noexcept {
  T& node = *it;
  XDCHECK(node.isInMMContainer());
  ++it;
  // rebalance should not be triggered inside this remove, because changing the
  // queues while having the EvictionIterator can cause inconsistency problem
  // for the iterator. Also, this remove is followed by an insertion into the
  // same container, which will trigger rebalance.
  removeLocked(node, /* doRebalance = */ false);
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
bool MM2Q::Container<T, HookPtr>::replace(T& oldNode, T& newNode) noexcept {
  return lruMutex_->lock_combine([this, &oldNode, &newNode]() {
    if (!oldNode.isInMMContainer() || newNode.isInMMContainer()) {
      return false;
    }
    const auto updateTime = getUpdateTime(oldNode);

    LruType type = getLruType(oldNode);
    lru_.getList(type).replace(oldNode, newNode);
    switch (type) {
    case LruType::Hot:
      markHot(newNode);
      break;
    case LruType::ColdTail:
      markTail(newNode); // pass through to also mark cold
    case LruType::Cold:
      markCold(newNode);
      break;
    case LruType::WarmTail:
      markTail(newNode);
      break; // warm is indicated by not marking hot or cold
    case LruType::Warm:
      break;
    case LruType::NumTypes:
      XDCHECK(false);
    }

    oldNode.unmarkInMMContainer();
    newNode.markInMMContainer();
    setUpdateTime(newNode, updateTime);
    return true;
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::LruType MM2Q::Container<T, HookPtr>::getTailLru(LruType list) const {
  switch (list) {
  case LruType::Warm:
    return LruType::WarmTail;
  case LruType::Cold:
    return LruType::ColdTail;
  default:
    XDCHECK(false);
    throw std::invalid_argument("The LRU list does not have a tail list");
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::adjustTail(LruType list) {
  auto tailList = getTailLru(list);
  auto ptr = lru_.getList(list).getTail();
  while (ptr && lru_.getList(tailList).size() + 1 <= config_.tailSize) {
    markTail(*ptr);
    lru_.getList(list).remove(*ptr);
    lru_.getList(tailList).linkAtHead(*ptr);
    ptr = lru_.getList(list).getTail();
  }
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
serialization::MM2QObject MM2Q::Container<T, HookPtr>::saveState()
    const noexcept {
  serialization::MM2QConfig configObject;
  *configObject.lruRefreshTime() = lruRefreshTime_;
  *configObject.lruRefreshRatio() = config_.lruRefreshRatio;
  *configObject.updateOnWrite() = config_.updateOnWrite;
  *configObject.updateOnRead() = config_.updateOnRead;
  *configObject.hotSizePercent() = config_.hotSizePercent;
  *configObject.coldSizePercent() = config_.coldSizePercent;
  *configObject.rebalanceOnRecordAccess() = config_.rebalanceOnRecordAccess;

  serialization::MM2QObject object;
  *object.config() = configObject;
  *object.tailTrackingEnabled() = tailTrackingEnabled_;
  *object.lrus() = lru_.saveState();
  return object;
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
MMContainerStat MM2Q::Container<T, HookPtr>::getStats() const noexcept {
  return lruMutex_->lock_combine([this]() {
    auto* tail = lru_.size() == 0 ? nullptr : lru_.rbegin().get();
    auto computeWeightedAccesses = [&](size_t warm, size_t cold) {
      return (warm * config_.getWarmSizePercent() +
              cold * config_.coldSizePercent) /
             100;
    };

    // note that in the analagous code in MMLru, we return an instance of
    // std::array<std::uint64_t, 6> and construct the MMContainerStat instance
    // outside the lock with some other data that is not required to be read
    // under a lock.  This is done there to take advantage of
    // folly::DistributedMutex's return-value inlining feature which coalesces
    // the write from the critical section with the atomic operations required
    // as part of the internal synchronization mechanism.  That then transfers
    // the synchronization signal as well as the return value in one single
    // cacheline invalidation message.  At the time of writing this decision was
    // based on an implementation-detail aware to the author - 48 bytes can be
    // coalesced with the synchronization signal in folly::DistributedMutex.
    //
    // we cannot do that here because this critical section returns more data
    // than can be coalesced internally by folly::DistributedMutex (> 48 bytes).
    // So we construct and return the entire object under the lock.
    return MMContainerStat{
        lru_.size(),
        tail == nullptr ? 0 : getUpdateTime(*tail),
        lruRefreshTime_.load(std::memory_order_relaxed),
        numHotAccesses_,
        numColdAccesses_,
        numWarmAccesses_,
        computeWeightedAccesses(numWarmTailAccesses_, numColdTailAccesses_)};
  });
}

template <typename T, MM2Q::Hook<T> T::*HookPtr>
void MM2Q::Container<T, HookPtr>::reconfigureLocked(const Time& currTime) {
  if (currTime < nextReconfigureTime_) {
    return;
  }
  nextReconfigureTime_ = currTime + config_.mmReconfigureIntervalSecs.count();

  // update LRU refresh time
  auto stat = getEvictionAgeStatLocked(0);
  auto lruRefreshTime = std::min(
      std::max(config_.defaultLruRefreshTime,
               static_cast<uint32_t>(stat.warmQueueStat.oldestElementAge *
                                     config_.lruRefreshRatio)),
      kLruRefreshTimeCap);
  lruRefreshTime_.store(lruRefreshTime, std::memory_order_relaxed);
}

// Iterator Context Implementation
template <typename T, MM2Q::Hook<T> T::*HookPtr>
MM2Q::Container<T, HookPtr>::LockedIterator::LockedIterator(
    LockHolder l, const Iterator& iter) noexcept
    : Iterator(iter), l_(std::move(l)) {}

} // namespace cachelib
} // namespace facebook
